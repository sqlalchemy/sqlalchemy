# postgresql/base.py
# Copyright (C) 2005-2016 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
.. dialect:: postgresql
    :name: PostgreSQL


Sequences/SERIAL
----------------

PostgreSQL supports sequences, and SQLAlchemy uses these as the default means
of creating new primary key values for integer-based primary key columns. When
creating tables, SQLAlchemy will issue the ``SERIAL`` datatype for
integer-based primary key columns, which generates a sequence and server side
default corresponding to the column.

To specify a specific named sequence to be used for primary key generation,
use the :func:`~sqlalchemy.schema.Sequence` construct::

    Table('sometable', metadata,
            Column('id', Integer, Sequence('some_id_seq'), primary_key=True)
        )

When SQLAlchemy issues a single INSERT statement, to fulfill the contract of
having the "last insert identifier" available, a RETURNING clause is added to
the INSERT statement which specifies the primary key columns should be
returned after the statement completes. The RETURNING functionality only takes
place if Postgresql 8.2 or later is in use. As a fallback approach, the
sequence, whether specified explicitly or implicitly via ``SERIAL``, is
executed independently beforehand, the returned value to be used in the
subsequent insert. Note that when an
:func:`~sqlalchemy.sql.expression.insert()` construct is executed using
"executemany" semantics, the "last inserted identifier" functionality does not
apply; no RETURNING clause is emitted nor is the sequence pre-executed in this
case.

To force the usage of RETURNING by default off, specify the flag
``implicit_returning=False`` to :func:`.create_engine`.

.. _postgresql_isolation_level:

Transaction Isolation Level
---------------------------

All Postgresql dialects support setting of transaction isolation level
both via a dialect-specific parameter :paramref:`.create_engine.isolation_level`
accepted by :func:`.create_engine`,
as well as the :paramref:`.Connection.execution_options.isolation_level` argument as passed to
:meth:`.Connection.execution_options`.  When using a non-psycopg2 dialect,
this feature works by issuing the command
``SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL <level>`` for
each new connection.  For the special AUTOCOMMIT isolation level, DBAPI-specific
techniques are used.

To set isolation level using :func:`.create_engine`::

    engine = create_engine(
        "postgresql+pg8000://scott:tiger@localhost/test",
        isolation_level="READ UNCOMMITTED"
    )

To set using per-connection execution options::

    connection = engine.connect()
    connection = connection.execution_options(
        isolation_level="READ COMMITTED"
    )

Valid values for ``isolation_level`` include:

* ``READ COMMITTED``
* ``READ UNCOMMITTED``
* ``REPEATABLE READ``
* ``SERIALIZABLE``
* ``AUTOCOMMIT`` - on psycopg2 / pg8000 only

.. seealso::

    :ref:`psycopg2_isolation_level`

    :ref:`pg8000_isolation_level`

.. _postgresql_schema_reflection:

Remote-Schema Table Introspection and Postgresql search_path
------------------------------------------------------------

The Postgresql dialect can reflect tables from any schema.  The
:paramref:`.Table.schema` argument, or alternatively the
:paramref:`.MetaData.reflect.schema` argument determines which schema will
be searched for the table or tables.   The reflected :class:`.Table` objects
will in all cases retain this ``.schema`` attribute as was specified.
However, with regards to tables which these :class:`.Table` objects refer to
via foreign key constraint, a decision must be made as to how the ``.schema``
is represented in those remote tables, in the case where that remote
schema name is also a member of the current
`Postgresql search path
<http://www.postgresql.org/docs/current/static/ddl-schemas.html#DDL-SCHEMAS-PATH>`_.

By default, the Postgresql dialect mimics the behavior encouraged by
Postgresql's own ``pg_get_constraintdef()`` builtin procedure.  This function
returns a sample definition for a particular foreign key constraint,
omitting the referenced schema name from that definition when the name is
also in the Postgresql schema search path.  The interaction below
illustrates this behavior::

    test=> CREATE TABLE test_schema.referred(id INTEGER PRIMARY KEY);
    CREATE TABLE
    test=> CREATE TABLE referring(
    test(>         id INTEGER PRIMARY KEY,
    test(>         referred_id INTEGER REFERENCES test_schema.referred(id));
    CREATE TABLE
    test=> SET search_path TO public, test_schema;
    test=> SELECT pg_catalog.pg_get_constraintdef(r.oid, true) FROM
    test-> pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n
    test-> ON n.oid = c.relnamespace
    test-> JOIN pg_catalog.pg_constraint r  ON c.oid = r.conrelid
    test-> WHERE c.relname='referring' AND r.contype = 'f'
    test-> ;
                   pg_get_constraintdef
    ---------------------------------------------------
     FOREIGN KEY (referred_id) REFERENCES referred(id)
    (1 row)

Above, we created a table ``referred`` as a member of the remote schema
``test_schema``, however when we added ``test_schema`` to the
PG ``search_path`` and then asked ``pg_get_constraintdef()`` for the
``FOREIGN KEY`` syntax, ``test_schema`` was not included in the output of
the function.

On the other hand, if we set the search path back to the typical default
of ``public``::

    test=> SET search_path TO public;
    SET

The same query against ``pg_get_constraintdef()`` now returns the fully
schema-qualified name for us::

    test=> SELECT pg_catalog.pg_get_constraintdef(r.oid, true) FROM
    test-> pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n
    test-> ON n.oid = c.relnamespace
    test-> JOIN pg_catalog.pg_constraint r  ON c.oid = r.conrelid
    test-> WHERE c.relname='referring' AND r.contype = 'f';
                         pg_get_constraintdef
    ---------------------------------------------------------------
     FOREIGN KEY (referred_id) REFERENCES test_schema.referred(id)
    (1 row)

SQLAlchemy will by default use the return value of ``pg_get_constraintdef()``
in order to determine the remote schema name.  That is, if our ``search_path``
were set to include ``test_schema``, and we invoked a table
reflection process as follows::

    >>> from sqlalchemy import Table, MetaData, create_engine
    >>> engine = create_engine("postgresql://scott:tiger@localhost/test")
    >>> with engine.connect() as conn:
    ...     conn.execute("SET search_path TO test_schema, public")
    ...     meta = MetaData()
    ...     referring = Table('referring', meta,
    ...                       autoload=True, autoload_with=conn)
    ...
    <sqlalchemy.engine.result.ResultProxy object at 0x101612ed0>

The above process would deliver to the :attr:`.MetaData.tables` collection
``referred`` table named **without** the schema::

    >>> meta.tables['referred'].schema is None
    True

To alter the behavior of reflection such that the referred schema is
maintained regardless of the ``search_path`` setting, use the
``postgresql_ignore_search_path`` option, which can be specified as a
dialect-specific argument to both :class:`.Table` as well as
:meth:`.MetaData.reflect`::

    >>> with engine.connect() as conn:
    ...     conn.execute("SET search_path TO test_schema, public")
    ...     meta = MetaData()
    ...     referring = Table('referring', meta, autoload=True,
    ...                       autoload_with=conn,
    ...                       postgresql_ignore_search_path=True)
    ...
    <sqlalchemy.engine.result.ResultProxy object at 0x1016126d0>

We will now have ``test_schema.referred`` stored as schema-qualified::

    >>> meta.tables['test_schema.referred'].schema
    'test_schema'

.. sidebar:: Best Practices for Postgresql Schema reflection

    The description of Postgresql schema reflection behavior is complex, and
    is the product of many years of dealing with widely varied use cases and
    user preferences. But in fact, there's no need to understand any of it if
    you just stick to the simplest use pattern: leave the ``search_path`` set
    to its default of ``public`` only, never refer to the name ``public`` as
    an explicit schema name otherwise, and refer to all other schema names
    explicitly when building up a :class:`.Table` object.  The options
    described here are only for those users who can't, or prefer not to, stay
    within these guidelines.

Note that **in all cases**, the "default" schema is always reflected as
``None``. The "default" schema on Postgresql is that which is returned by the
Postgresql ``current_schema()`` function.  On a typical Postgresql
installation, this is the name ``public``.  So a table that refers to another
which is in the ``public`` (i.e. default) schema will always have the
``.schema`` attribute set to ``None``.

.. versionadded:: 0.9.2 Added the ``postgresql_ignore_search_path``
   dialect-level option accepted by :class:`.Table` and
   :meth:`.MetaData.reflect`.


.. seealso::

        `The Schema Search Path
        <http://www.postgresql.org/docs/9.0/static/ddl-schemas.html#DDL-SCHEMAS-PATH>`_
        - on the Postgresql website.

INSERT/UPDATE...RETURNING
-------------------------

The dialect supports PG 8.2's ``INSERT..RETURNING``, ``UPDATE..RETURNING`` and
``DELETE..RETURNING`` syntaxes.   ``INSERT..RETURNING`` is used by default
for single-row INSERT statements in order to fetch newly generated
primary key identifiers.   To specify an explicit ``RETURNING`` clause,
use the :meth:`._UpdateBase.returning` method on a per-statement basis::

    # INSERT..RETURNING
    result = table.insert().returning(table.c.col1, table.c.col2).\\
        values(name='foo')
    print result.fetchall()

    # UPDATE..RETURNING
    result = table.update().returning(table.c.col1, table.c.col2).\\
        where(table.c.name=='foo').values(name='bar')
    print result.fetchall()

    # DELETE..RETURNING
    result = table.delete().returning(table.c.col1, table.c.col2).\\
        where(table.c.name=='foo')
    print result.fetchall()

.. _postgresql_match:

Full Text Search
----------------

SQLAlchemy makes available the Postgresql ``@@`` operator via the
:meth:`.ColumnElement.match` method on any textual column expression.
On a Postgresql dialect, an expression like the following::

    select([sometable.c.text.match("search string")])

will emit to the database::

    SELECT text @@ to_tsquery('search string') FROM table

The Postgresql text search functions such as ``to_tsquery()``
and ``to_tsvector()`` are available
explicitly using the standard :data:`.func` construct.  For example::

    select([
        func.to_tsvector('fat cats ate rats').match('cat & rat')
    ])

Emits the equivalent of::

    SELECT to_tsvector('fat cats ate rats') @@ to_tsquery('cat & rat')

The :class:`.postgresql.TSVECTOR` type can provide for explicit CAST::

    from sqlalchemy.dialects.postgresql import TSVECTOR
    from sqlalchemy import select, cast
    select([cast("some text", TSVECTOR)])

produces a statement equivalent to::

    SELECT CAST('some text' AS TSVECTOR) AS anon_1

Full Text Searches in Postgresql are influenced by a combination of: the
PostgresSQL setting of ``default_text_search_config``, the ``regconfig`` used
to build the GIN/GiST indexes, and the ``regconfig`` optionally passed in
during a query.

When performing a Full Text Search against a column that has a GIN or
GiST index that is already pre-computed (which is common on full text
searches) one may need to explicitly pass in a particular PostgresSQL
``regconfig`` value to ensure the query-planner utilizes the index and does
not re-compute the column on demand.

In order to provide for this explicit query planning, or to use different
search strategies, the ``match`` method accepts a ``postgresql_regconfig``
keyword argument::

    select([mytable.c.id]).where(
        mytable.c.title.match('somestring', postgresql_regconfig='english')
    )

Emits the equivalent of::

    SELECT mytable.id FROM mytable
    WHERE mytable.title @@ to_tsquery('english', 'somestring')

One can also specifically pass in a `'regconfig'` value to the
``to_tsvector()`` command as the initial argument::

    select([mytable.c.id]).where(
            func.to_tsvector('english', mytable.c.title )\
            .match('somestring', postgresql_regconfig='english')
        )

produces a statement equivalent to::

    SELECT mytable.id FROM mytable
    WHERE to_tsvector('english', mytable.title) @@
        to_tsquery('english', 'somestring')

It is recommended that you use the ``EXPLAIN ANALYZE...`` tool from
PostgresSQL to ensure that you are generating queries with SQLAlchemy that
take full advantage of any indexes you may have created for full text search.

FROM ONLY ...
------------------------

The dialect supports PostgreSQL's ONLY keyword for targeting only a particular
table in an inheritance hierarchy. This can be used to produce the
``SELECT ... FROM ONLY``, ``UPDATE ONLY ...``, and ``DELETE FROM ONLY ...``
syntaxes. It uses SQLAlchemy's hints mechanism::

    # SELECT ... FROM ONLY ...
    result = table.select().with_hint(table, 'ONLY', 'postgresql')
    print result.fetchall()

    # UPDATE ONLY ...
    table.update(values=dict(foo='bar')).with_hint('ONLY',
                                                   dialect_name='postgresql')

    # DELETE FROM ONLY ...
    table.delete().with_hint('ONLY', dialect_name='postgresql')

.. _postgresql_indexes:

Postgresql-Specific Index Options
---------------------------------

Several extensions to the :class:`.Index` construct are available, specific
to the PostgreSQL dialect.

Partial Indexes
^^^^^^^^^^^^^^^^

Partial indexes add criterion to the index definition so that the index is
applied to a subset of rows.   These can be specified on :class:`.Index`
using the ``postgresql_where`` keyword argument::

  Index('my_index', my_table.c.id, postgresql_where=tbl.c.value > 10)

Operator Classes
^^^^^^^^^^^^^^^^^

PostgreSQL allows the specification of an *operator class* for each column of
an index (see
http://www.postgresql.org/docs/8.3/interactive/indexes-opclass.html).
The :class:`.Index` construct allows these to be specified via the
``postgresql_ops`` keyword argument::

    Index('my_index', my_table.c.id, my_table.c.data,
                            postgresql_ops={
                                'data': 'text_pattern_ops',
                                'id': 'int4_ops'
                            })

.. versionadded:: 0.7.2
    ``postgresql_ops`` keyword argument to :class:`.Index` construct.

Note that the keys in the ``postgresql_ops`` dictionary are the "key" name of
the :class:`.Column`, i.e. the name used to access it from the ``.c``
collection of :class:`.Table`, which can be configured to be different than
the actual name of the column as expressed in the database.

Index Types
^^^^^^^^^^^^

PostgreSQL provides several index types: B-Tree, Hash, GiST, and GIN, as well
as the ability for users to create their own (see
http://www.postgresql.org/docs/8.3/static/indexes-types.html). These can be
specified on :class:`.Index` using the ``postgresql_using`` keyword argument::

    Index('my_index', my_table.c.data, postgresql_using='gin')

The value passed to the keyword argument will be simply passed through to the
underlying CREATE INDEX command, so it *must* be a valid index type for your
version of PostgreSQL.

.. _postgresql_index_storage:

Index Storage Parameters
^^^^^^^^^^^^^^^^^^^^^^^^

PostgreSQL allows storage parameters to be set on indexes. The storage
parameters available depend on the index method used by the index. Storage
parameters can be specified on :class:`.Index` using the ``postgresql_with``
keyword argument::

    Index('my_index', my_table.c.data, postgresql_with={"fillfactor": 50})

.. versionadded:: 1.0.6

.. _postgresql_index_concurrently:

Indexes with CONCURRENTLY
^^^^^^^^^^^^^^^^^^^^^^^^^

The Postgresql index option CONCURRENTLY is supported by passing the
flag ``postgresql_concurrently`` to the :class:`.Index` construct::

    tbl = Table('testtbl', m, Column('data', Integer))

    idx1 = Index('test_idx1', tbl.c.data, postgresql_concurrently=True)

The above index construct will render SQL as::

    CREATE INDEX CONCURRENTLY test_idx1 ON testtbl (data)

.. versionadded:: 0.9.9

.. _postgresql_index_reflection:

Postgresql Index Reflection
---------------------------

The Postgresql database creates a UNIQUE INDEX implicitly whenever the
UNIQUE CONSTRAINT construct is used.   When inspecting a table using
:class:`.Inspector`, the :meth:`.Inspector.get_indexes`
and the :meth:`.Inspector.get_unique_constraints` will report on these
two constructs distinctly; in the case of the index, the key
``duplicates_constraint`` will be present in the index entry if it is
detected as mirroring a constraint.   When performing reflection using
``Table(..., autoload=True)``, the UNIQUE INDEX is **not** returned
in :attr:`.Table.indexes` when it is detected as mirroring a
:class:`.UniqueConstraint` in the :attr:`.Table.constraints` collection.

.. versionchanged:: 1.0.0 - :class:`.Table` reflection now includes
   :class:`.UniqueConstraint` objects present in the :attr:`.Table.constraints`
   collection; the Postgresql backend will no longer include a "mirrored"
   :class:`.Index` construct in :attr:`.Table.indexes` if it is detected
   as corresponding to a unique constraint.

Special Reflection Options
--------------------------

The :class:`.Inspector` used for the Postgresql backend is an instance
of :class:`.PGInspector`, which offers additional methods::

    from sqlalchemy import create_engine, inspect

    engine = create_engine("postgresql+psycopg2://localhost/test")
    insp = inspect(engine)  # will be a PGInspector

    print(insp.get_enums())

.. autoclass:: PGInspector
    :members:

.. _postgresql_table_options:

PostgreSQL Table Options
-------------------------

Several options for CREATE TABLE are supported directly by the PostgreSQL
dialect in conjunction with the :class:`.Table` construct:

* ``TABLESPACE``::

    Table("some_table", metadata, ..., postgresql_tablespace='some_tablespace')

* ``ON COMMIT``::

    Table("some_table", metadata, ..., postgresql_on_commit='PRESERVE ROWS')

* ``WITH OIDS``::

    Table("some_table", metadata, ..., postgresql_with_oids=True)

* ``WITHOUT OIDS``::

    Table("some_table", metadata, ..., postgresql_with_oids=False)

* ``INHERITS``::

    Table("some_table", metadata, ..., postgresql_inherits="some_supertable")

    Table("some_table", metadata, ..., postgresql_inherits=("t1", "t2", ...))

.. versionadded:: 1.0.0

.. seealso::

    `Postgresql CREATE TABLE options
    <http://www.postgresql.org/docs/current/static/sql-createtable.html>`_

ENUM Types
----------

Postgresql has an independently creatable TYPE structure which is used
to implement an enumerated type.   This approach introduces significant
complexity on the SQLAlchemy side in terms of when this type should be
CREATED and DROPPED.   The type object is also an independently reflectable
entity.   The following sections should be consulted:

* :class:`.postgresql.ENUM` - DDL and typing support for ENUM.

* :meth:`.PGInspector.get_enums` - retrieve a listing of current ENUM types

* :meth:`.postgresql.ENUM.create` , :meth:`.postgresql.ENUM.drop` - individual
  CREATE and DROP commands for ENUM.

.. _postgresql_array_of_enum:

Using ENUM with ARRAY
^^^^^^^^^^^^^^^^^^^^^

The combination of ENUM and ARRAY is not directly supported by backend
DBAPIs at this time.   In order to send and receive an ARRAY of ENUM,
use the following workaround type::

    class ArrayOfEnum(ARRAY):

        def bind_expression(self, bindvalue):
            return sa.cast(bindvalue, self)

        def result_processor(self, dialect, coltype):
            super_rp = super(ArrayOfEnum, self).result_processor(
                dialect, coltype)

            def handle_raw_string(value):
                inner = re.match(r"^{(.*)}$", value).group(1)
                return inner.split(",") if inner else []

            def process(value):
                if value is None:
                    return None
                return super_rp(handle_raw_string(value))
            return process

E.g.::

    Table(
        'mydata', metadata,
        Column('id', Integer, primary_key=True),
        Column('data', ArrayOfEnum(ENUM('a', 'b, 'c', name='myenum')))

    )

This type is not included as a built-in type as it would be incompatible
with a DBAPI that suddenly decides to support ARRAY of ENUM directly in
a new version.

"""
from collections import defaultdict
import re
import datetime as dt


from ... import sql, schema, exc, util
from ...engine import default, reflection
from ...sql import compiler, expression, operators, default_comparator
from ... import types as sqltypes

try:
    from uuid import UUID as _python_UUID
except ImportError:
    _python_UUID = None

from sqlalchemy.types import INTEGER, BIGINT, SMALLINT, VARCHAR, \
    CHAR, TEXT, FLOAT, NUMERIC, \
    DATE, BOOLEAN, REAL

RESERVED_WORDS = set(
    ["all", "analyse", "analyze", "and", "any", "array", "as", "asc",
     "asymmetric", "both", "case", "cast", "check", "collate", "column",
     "constraint", "create", "current_catalog", "current_date",
     "current_role", "current_time", "current_timestamp", "current_user",
     "default", "deferrable", "desc", "distinct", "do", "else", "end",
     "except", "false", "fetch", "for", "foreign", "from", "grant", "group",
     "having", "in", "initially", "intersect", "into", "leading", "limit",
     "localtime", "localtimestamp", "new", "not", "null", "of", "off",
     "offset", "old", "on", "only", "or", "order", "placing", "primary",
     "references", "returning", "select", "session_user", "some", "symmetric",
     "table", "then", "to", "trailing", "true", "union", "unique", "user",
     "using", "variadic", "when", "where", "window", "with", "authorization",
     "between", "binary", "cross", "current_schema", "freeze", "full",
     "ilike", "inner", "is", "isnull", "join", "left", "like", "natural",
     "notnull", "outer", "over", "overlaps", "right", "similar", "verbose"
     ])

_DECIMAL_TYPES = (1231, 1700)
_FLOAT_TYPES = (700, 701, 1021, 1022)
_INT_TYPES = (20, 21, 23, 26, 1005, 1007, 1016)


class BYTEA(sqltypes.LargeBinary):
    __visit_name__ = 'BYTEA'


class DOUBLE_PRECISION(sqltypes.Float):
    __visit_name__ = 'DOUBLE_PRECISION'


class INET(sqltypes.TypeEngine):
    __visit_name__ = "INET"
PGInet = INET


class CIDR(sqltypes.TypeEngine):
    __visit_name__ = "CIDR"
PGCidr = CIDR


class MACADDR(sqltypes.TypeEngine):
    __visit_name__ = "MACADDR"
PGMacAddr = MACADDR


class OID(sqltypes.TypeEngine):

    """Provide the Postgresql OID type.

    .. versionadded:: 0.9.5

    """
    __visit_name__ = "OID"


class TIMESTAMP(sqltypes.TIMESTAMP):

    def __init__(self, timezone=False, precision=None):
        super(TIMESTAMP, self).__init__(timezone=timezone)
        self.precision = precision


class TIME(sqltypes.TIME):

    def __init__(self, timezone=False, precision=None):
        super(TIME, self).__init__(timezone=timezone)
        self.precision = precision


class INTERVAL(sqltypes.TypeEngine):

    """Postgresql INTERVAL type.

    The INTERVAL type may not be supported on all DBAPIs.
    It is known to work on psycopg2 and not pg8000 or zxjdbc.

    """
    __visit_name__ = 'INTERVAL'

    def __init__(self, precision=None):
        self.precision = precision

    @classmethod
    def _adapt_from_generic_interval(cls, interval):
        return INTERVAL(precision=interval.second_precision)

    @property
    def _type_affinity(self):
        return sqltypes.Interval

    @property
    def python_type(self):
        return dt.timedelta

PGInterval = INTERVAL


class BIT(sqltypes.TypeEngine):
    __visit_name__ = 'BIT'

    def __init__(self, length=None, varying=False):
        if not varying:
            # BIT without VARYING defaults to length 1
            self.length = length or 1
        else:
            # but BIT VARYING can be unlimited-length, so no default
            self.length = length
        self.varying = varying

PGBit = BIT


class UUID(sqltypes.TypeEngine):

    """Postgresql UUID type.

    Represents the UUID column type, interpreting
    data either as natively returned by the DBAPI
    or as Python uuid objects.

    The UUID type may not be supported on all DBAPIs.
    It is known to work on psycopg2 and not pg8000.

    """
    __visit_name__ = 'UUID'

    def __init__(self, as_uuid=False):
        """Construct a UUID type.


        :param as_uuid=False: if True, values will be interpreted
         as Python uuid objects, converting to/from string via the
         DBAPI.

         """
        if as_uuid and _python_UUID is None:
            raise NotImplementedError(
                "This version of Python does not support "
                "the native UUID type."
            )
        self.as_uuid = as_uuid

    def bind_processor(self, dialect):
        if self.as_uuid:
            def process(value):
                if value is not None:
                    value = util.text_type(value)
                return value
            return process
        else:
            return None

    def result_processor(self, dialect, coltype):
        if self.as_uuid:
            def process(value):
                if value is not None:
                    value = _python_UUID(value)
                return value
            return process
        else:
            return None

PGUuid = UUID


class TSVECTOR(sqltypes.TypeEngine):

    """The :class:`.postgresql.TSVECTOR` type implements the Postgresql
    text search type TSVECTOR.

    It can be used to do full text queries on natural language
    documents.

    .. versionadded:: 0.9.0

    .. seealso::

        :ref:`postgresql_match`

    """
    __visit_name__ = 'TSVECTOR'


class _Slice(expression.ColumnElement):
    __visit_name__ = 'slice'
    type = sqltypes.NULLTYPE

    def __init__(self, slice_, source_comparator):
        self.start = default_comparator._check_literal(
            source_comparator.expr,
            operators.getitem, slice_.start)
        self.stop = default_comparator._check_literal(
            source_comparator.expr,
            operators.getitem, slice_.stop)


class Any(expression.ColumnElement):

    """Represent the clause ``left operator ANY (right)``.  ``right`` must be
    an array expression.

    .. seealso::

        :class:`.postgresql.ARRAY`

        :meth:`.postgresql.ARRAY.Comparator.any` - ARRAY-bound method

    """
    __visit_name__ = 'any'

    def __init__(self, left, right, operator=operators.eq):
        self.type = sqltypes.Boolean()
        self.left = expression._literal_as_binds(left)
        self.right = right
        self.operator = operator


class All(expression.ColumnElement):

    """Represent the clause ``left operator ALL (right)``.  ``right`` must be
    an array expression.

    .. seealso::

        :class:`.postgresql.ARRAY`

        :meth:`.postgresql.ARRAY.Comparator.all` - ARRAY-bound method

    """
    __visit_name__ = 'all'

    def __init__(self, left, right, operator=operators.eq):
        self.type = sqltypes.Boolean()
        self.left = expression._literal_as_binds(left)
        self.right = right
        self.operator = operator


class array(expression.Tuple):

    """A Postgresql ARRAY literal.

    This is used to produce ARRAY literals in SQL expressions, e.g.::

        from sqlalchemy.dialects.postgresql import array
        from sqlalchemy.dialects import postgresql
        from sqlalchemy import select, func

        stmt = select([
                        array([1,2]) + array([3,4,5])
                    ])

        print stmt.compile(dialect=postgresql.dialect())

    Produces the SQL::

        SELECT ARRAY[%(param_1)s, %(param_2)s] ||
            ARRAY[%(param_3)s, %(param_4)s, %(param_5)s]) AS anon_1

    An instance of :class:`.array` will always have the datatype
    :class:`.ARRAY`.  The "inner" type of the array is inferred from
    the values present, unless the ``type_`` keyword argument is passed::

        array(['foo', 'bar'], type_=CHAR)

    .. versionadded:: 0.8 Added the :class:`~.postgresql.array` literal type.

    See also:

    :class:`.postgresql.ARRAY`

    """
    __visit_name__ = 'array'

    def __init__(self, clauses, **kw):
        super(array, self).__init__(*clauses, **kw)
        self.type = ARRAY(self.type)

    def _bind_param(self, operator, obj):
        return array([
            expression.BindParameter(None, o, _compared_to_operator=operator,
                                     _compared_to_type=self.type, unique=True)
            for o in obj
        ])

    def self_group(self, against=None):
        return self


class ARRAY(sqltypes.Concatenable, sqltypes.TypeEngine):

    """Postgresql ARRAY type.

    Represents values as Python lists.

    An :class:`.ARRAY` type is constructed given the "type"
    of element::

        mytable = Table("mytable", metadata,
                Column("data", ARRAY(Integer))
            )

    The above type represents an N-dimensional array,
    meaning Postgresql will interpret values with any number
    of dimensions automatically.   To produce an INSERT
    construct that passes in a 1-dimensional array of integers::

        connection.execute(
                mytable.insert(),
                data=[1,2,3]
        )

    The :class:`.ARRAY` type can be constructed given a fixed number
    of dimensions::

        mytable = Table("mytable", metadata,
                Column("data", ARRAY(Integer, dimensions=2))
            )

    This has the effect of the :class:`.ARRAY` type
    specifying that number of bracketed blocks when a :class:`.Table`
    is used in a CREATE TABLE statement, or when the type is used
    within a :func:`.expression.cast` construct; it also causes
    the bind parameter and result set processing of the type
    to optimize itself to expect exactly that number of dimensions.
    Note that Postgresql itself still allows N dimensions with such a type.

    SQL expressions of type :class:`.ARRAY` have support for "index" and
    "slice" behavior.  The Python ``[]`` operator works normally here, given
    integer indexes or slices.  Note that Postgresql arrays default
    to 1-based indexing.  The operator produces binary expression
    constructs which will produce the appropriate SQL, both for
    SELECT statements::

        select([mytable.c.data[5], mytable.c.data[2:7]])

    as well as UPDATE statements when the :meth:`.Update.values` method
    is used::

        mytable.update().values({
            mytable.c.data[5]: 7,
            mytable.c.data[2:7]: [1, 2, 3]
        })

    .. note::

        Multi-dimensional support for the ``[]`` operator is not supported
        in SQLAlchemy 1.0.  Please use the :func:`.type_coerce` function
        to cast an intermediary expression to ARRAY again as a workaround::

            expr = type_coerce(my_array_column[5], ARRAY(Integer))[6]

        Multi-dimensional support will be provided in a future release.

    :class:`.ARRAY` provides special methods for containment operations,
    e.g.::

        mytable.c.data.contains([1, 2])

    For a full list of special methods see :class:`.ARRAY.Comparator`.

    .. versionadded:: 0.8 Added support for index and slice operations
       to the :class:`.ARRAY` type, including support for UPDATE
       statements, and special array containment operations.

    The :class:`.ARRAY` type may not be supported on all DBAPIs.
    It is known to work on psycopg2 and not pg8000.

    Additionally, the :class:`.ARRAY` type does not work directly in
    conjunction with the :class:`.ENUM` type.  For a workaround, see the
    special type at :ref:`postgresql_array_of_enum`.

    See also:

    :class:`.postgresql.array` - produce a literal array value.

    """
    __visit_name__ = 'ARRAY'

    class Comparator(sqltypes.Concatenable.Comparator):

        """Define comparison operations for :class:`.ARRAY`."""

        def __getitem__(self, index):
            shift_indexes = 1 if self.expr.type.zero_indexes else 0
            if isinstance(index, slice):
                if shift_indexes:
                    index = slice(
                        index.start + shift_indexes,
                        index.stop + shift_indexes,
                        index.step
                    )
                index = _Slice(index, self)
                return_type = self.type
            else:
                index += shift_indexes
                return_type = self.type.item_type

            return default_comparator._binary_operate(
                self.expr, operators.getitem, index,
                result_type=return_type)

        def any(self, other, operator=operators.eq):
            """Return ``other operator ANY (array)`` clause.

            Argument places are switched, because ANY requires array
            expression to be on the right hand-side.

            E.g.::

                from sqlalchemy.sql import operators

                conn.execute(
                    select([table.c.data]).where(
                            table.c.data.any(7, operator=operators.lt)
                        )
                )

            :param other: expression to be compared
            :param operator: an operator object from the
             :mod:`sqlalchemy.sql.operators`
             package, defaults to :func:`.operators.eq`.

            .. seealso::

                :class:`.postgresql.Any`

                :meth:`.postgresql.ARRAY.Comparator.all`

            """
            return Any(other, self.expr, operator=operator)

        def all(self, other, operator=operators.eq):
            """Return ``other operator ALL (array)`` clause.

            Argument places are switched, because ALL requires array
            expression to be on the right hand-side.

            E.g.::

                from sqlalchemy.sql import operators

                conn.execute(
                    select([table.c.data]).where(
                            table.c.data.all(7, operator=operators.lt)
                        )
                )

            :param other: expression to be compared
            :param operator: an operator object from the
             :mod:`sqlalchemy.sql.operators`
             package, defaults to :func:`.operators.eq`.

            .. seealso::

                :class:`.postgresql.All`

                :meth:`.postgresql.ARRAY.Comparator.any`

            """
            return All(other, self.expr, operator=operator)

        def contains(self, other, **kwargs):
            """Boolean expression.  Test if elements are a superset of the
            elements of the argument array expression.
            """
            return self.expr.op('@>')(other)

        def contained_by(self, other):
            """Boolean expression.  Test if elements are a proper subset of the
            elements of the argument array expression.
            """
            return self.expr.op('<@')(other)

        def overlap(self, other):
            """Boolean expression.  Test if array has elements in common with
            an argument array expression.
            """
            return self.expr.op('&&')(other)

        def _adapt_expression(self, op, other_comparator):
            if isinstance(op, operators.custom_op):
                if op.opstring in ['@>', '<@', '&&']:
                    return op, sqltypes.Boolean
            return sqltypes.Concatenable.Comparator.\
                _adapt_expression(self, op, other_comparator)

    comparator_factory = Comparator

    def __init__(self, item_type, as_tuple=False, dimensions=None,
                 zero_indexes=False):
        """Construct an ARRAY.

        E.g.::

          Column('myarray', ARRAY(Integer))

        Arguments are:

        :param item_type: The data type of items of this array. Note that
          dimensionality is irrelevant here, so multi-dimensional arrays like
          ``INTEGER[][]``, are constructed as ``ARRAY(Integer)``, not as
          ``ARRAY(ARRAY(Integer))`` or such.

        :param as_tuple=False: Specify whether return results
          should be converted to tuples from lists. DBAPIs such
          as psycopg2 return lists by default. When tuples are
          returned, the results are hashable.

        :param dimensions: if non-None, the ARRAY will assume a fixed
         number of dimensions.  This will cause the DDL emitted for this
         ARRAY to include the exact number of bracket clauses ``[]``,
         and will also optimize the performance of the type overall.
         Note that PG arrays are always implicitly "non-dimensioned",
         meaning they can store any number of dimensions no matter how
         they were declared.

        :param zero_indexes=False: when True, index values will be converted
         between Python zero-based and Postgresql one-based indexes, e.g.
         a value of one will be added to all index values before passing
         to the database.

         .. versionadded:: 0.9.5

        """
        if isinstance(item_type, ARRAY):
            raise ValueError("Do not nest ARRAY types; ARRAY(basetype) "
                             "handles multi-dimensional arrays of basetype")
        if isinstance(item_type, type):
            item_type = item_type()
        self.item_type = item_type
        self.as_tuple = as_tuple
        self.dimensions = dimensions
        self.zero_indexes = zero_indexes

    @property
    def python_type(self):
        return list

    def compare_values(self, x, y):
        return x == y

    def _proc_array(self, arr, itemproc, dim, collection):
        if dim is None:
            arr = list(arr)
        if dim == 1 or dim is None and (
                # this has to be (list, tuple), or at least
                # not hasattr('__iter__'), since Py3K strings
                # etc. have __iter__
                not arr or not isinstance(arr[0], (list, tuple))):
            if itemproc:
                return collection(itemproc(x) for x in arr)
            else:
                return collection(arr)
        else:
            return collection(
                self._proc_array(
                    x, itemproc,
                    dim - 1 if dim is not None else None,
                    collection)
                for x in arr
            )

    def bind_processor(self, dialect):
        item_proc = self.item_type.\
            dialect_impl(dialect).\
            bind_processor(dialect)

        def process(value):
            if value is None:
                return value
            else:
                return self._proc_array(
                    value,
                    item_proc,
                    self.dimensions,
                    list)
        return process

    def result_processor(self, dialect, coltype):
        item_proc = self.item_type.\
            dialect_impl(dialect).\
            result_processor(dialect, coltype)

        def process(value):
            if value is None:
                return value
            else:
                return self._proc_array(
                    value,
                    item_proc,
                    self.dimensions,
                    tuple if self.as_tuple else list)
        return process

PGArray = ARRAY


class ENUM(sqltypes.Enum):

    """Postgresql ENUM type.

    This is a subclass of :class:`.types.Enum` which includes
    support for PG's ``CREATE TYPE`` and ``DROP TYPE``.

    When the builtin type :class:`.types.Enum` is used and the
    :paramref:`.Enum.native_enum` flag is left at its default of
    True, the Postgresql backend will use a :class:`.postgresql.ENUM`
    type as the implementation, so the special create/drop rules
    will be used.

    The create/drop behavior of ENUM is necessarily intricate, due to the
    awkward relationship the ENUM type has in relationship to the
    parent table, in that it may be "owned" by just a single table, or
    may be shared among many tables.

    When using :class:`.types.Enum` or :class:`.postgresql.ENUM`
    in an "inline" fashion, the ``CREATE TYPE`` and ``DROP TYPE`` is emitted
    corresponding to when the :meth:`.Table.create` and :meth:`.Table.drop`
    methods are called::

        table = Table('sometable', metadata,
            Column('some_enum', ENUM('a', 'b', 'c', name='myenum'))
        )

        table.create(engine)  # will emit CREATE ENUM and CREATE TABLE
        table.drop(engine)  # will emit DROP TABLE and DROP ENUM

    To use a common enumerated type between multiple tables, the best
    practice is to declare the :class:`.types.Enum` or
    :class:`.postgresql.ENUM` independently, and associate it with the
    :class:`.MetaData` object itself::

        my_enum = ENUM('a', 'b', 'c', name='myenum', metadata=metadata)

        t1 = Table('sometable_one', metadata,
            Column('some_enum', myenum)
        )

        t2 = Table('sometable_two', metadata,
            Column('some_enum', myenum)
        )

    When this pattern is used, care must still be taken at the level
    of individual table creates.  Emitting CREATE TABLE without also
    specifying ``checkfirst=True`` will still cause issues::

        t1.create(engine) # will fail: no such type 'myenum'

    If we specify ``checkfirst=True``, the individual table-level create
    operation will check for the ``ENUM`` and create if not exists::

        # will check if enum exists, and emit CREATE TYPE if not
        t1.create(engine, checkfirst=True)

    When using a metadata-level ENUM type, the type will always be created
    and dropped if either the metadata-wide create/drop is called::

        metadata.create_all(engine)  # will emit CREATE TYPE
        metadata.drop_all(engine)  # will emit DROP TYPE

    The type can also be created and dropped directly::

        my_enum.create(engine)
        my_enum.drop(engine)

    .. versionchanged:: 1.0.0 The Postgresql :class:`.postgresql.ENUM` type
       now behaves more strictly with regards to CREATE/DROP.  A metadata-level
       ENUM type will only be created and dropped at the metadata level,
       not the table level, with the exception of
       ``table.create(checkfirst=True)``.
       The ``table.drop()`` call will now emit a DROP TYPE for a table-level
       enumerated type.

    """

    def __init__(self, *enums, **kw):
        """Construct an :class:`~.postgresql.ENUM`.

        Arguments are the same as that of
        :class:`.types.Enum`, but also including
        the following parameters.

        :param create_type: Defaults to True.
         Indicates that ``CREATE TYPE`` should be
         emitted, after optionally checking for the
         presence of the type, when the parent
         table is being created; and additionally
         that ``DROP TYPE`` is called when the table
         is dropped.    When ``False``, no check
         will be performed and no ``CREATE TYPE``
         or ``DROP TYPE`` is emitted, unless
         :meth:`~.postgresql.ENUM.create`
         or :meth:`~.postgresql.ENUM.drop`
         are called directly.
         Setting to ``False`` is helpful
         when invoking a creation scheme to a SQL file
         without access to the actual database -
         the :meth:`~.postgresql.ENUM.create` and
         :meth:`~.postgresql.ENUM.drop` methods can
         be used to emit SQL to a target bind.

         .. versionadded:: 0.7.4

        """
        self.create_type = kw.pop("create_type", True)
        super(ENUM, self).__init__(*enums, **kw)

    def create(self, bind=None, checkfirst=True):
        """Emit ``CREATE TYPE`` for this
        :class:`~.postgresql.ENUM`.

        If the underlying dialect does not support
        Postgresql CREATE TYPE, no action is taken.

        :param bind: a connectable :class:`.Engine`,
         :class:`.Connection`, or similar object to emit
         SQL.
        :param checkfirst: if ``True``, a query against
         the PG catalog will be first performed to see
         if the type does not exist already before
         creating.

        """
        if not bind.dialect.supports_native_enum:
            return

        if not checkfirst or \
                not bind.dialect.has_type(
                    bind, self.name, schema=self.schema):
            bind.execute(CreateEnumType(self))

    def drop(self, bind=None, checkfirst=True):
        """Emit ``DROP TYPE`` for this
        :class:`~.postgresql.ENUM`.

        If the underlying dialect does not support
        Postgresql DROP TYPE, no action is taken.

        :param bind: a connectable :class:`.Engine`,
         :class:`.Connection`, or similar object to emit
         SQL.
        :param checkfirst: if ``True``, a query against
         the PG catalog will be first performed to see
         if the type actually exists before dropping.

        """
        if not bind.dialect.supports_native_enum:
            return

        if not checkfirst or \
                bind.dialect.has_type(bind, self.name, schema=self.schema):
            bind.execute(DropEnumType(self))

    def _check_for_name_in_memos(self, checkfirst, kw):
        """Look in the 'ddl runner' for 'memos', then
        note our name in that collection.

        This to ensure a particular named enum is operated
        upon only once within any kind of create/drop
        sequence without relying upon "checkfirst".

        """
        if not self.create_type:
            return True
        if '_ddl_runner' in kw:
            ddl_runner = kw['_ddl_runner']
            if '_pg_enums' in ddl_runner.memo:
                pg_enums = ddl_runner.memo['_pg_enums']
            else:
                pg_enums = ddl_runner.memo['_pg_enums'] = set()
            present = self.name in pg_enums
            pg_enums.add(self.name)
            return present
        else:
            return False

    def _on_table_create(self, target, bind, checkfirst, **kw):
        if checkfirst or (
                not self.metadata and
                not kw.get('_is_metadata_operation', False)) and \
                not self._check_for_name_in_memos(checkfirst, kw):
            self.create(bind=bind, checkfirst=checkfirst)

    def _on_table_drop(self, target, bind, checkfirst, **kw):
        if not self.metadata and \
            not kw.get('_is_metadata_operation', False) and \
                not self._check_for_name_in_memos(checkfirst, kw):
            self.drop(bind=bind, checkfirst=checkfirst)

    def _on_metadata_create(self, target, bind, checkfirst, **kw):
        if not self._check_for_name_in_memos(checkfirst, kw):
            self.create(bind=bind, checkfirst=checkfirst)

    def _on_metadata_drop(self, target, bind, checkfirst, **kw):
        if not self._check_for_name_in_memos(checkfirst, kw):
            self.drop(bind=bind, checkfirst=checkfirst)

colspecs = {
    sqltypes.Interval: INTERVAL,
    sqltypes.Enum: ENUM,
}

ischema_names = {
    'integer': INTEGER,
    'bigint': BIGINT,
    'smallint': SMALLINT,
    'character varying': VARCHAR,
    'character': CHAR,
    '"char"': sqltypes.String,
    'name': sqltypes.String,
    'text': TEXT,
    'numeric': NUMERIC,
    'float': FLOAT,
    'real': REAL,
    'inet': INET,
    'cidr': CIDR,
    'uuid': UUID,
    'bit': BIT,
    'bit varying': BIT,
    'macaddr': MACADDR,
    'oid': OID,
    'double precision': DOUBLE_PRECISION,
    'timestamp': TIMESTAMP,
    'timestamp with time zone': TIMESTAMP,
    'timestamp without time zone': TIMESTAMP,
    'time with time zone': TIME,
    'time without time zone': TIME,
    'date': DATE,
    'time': TIME,
    'bytea': BYTEA,
    'boolean': BOOLEAN,
    'interval': INTERVAL,
    'interval year to month': INTERVAL,
    'interval day to second': INTERVAL,
    'tsvector': TSVECTOR
}


class PGCompiler(compiler.SQLCompiler):

    def visit_array(self, element, **kw):
        return "ARRAY[%s]" % self.visit_clauselist(element, **kw)

    def visit_slice(self, element, **kw):
        return "%s:%s" % (
            self.process(element.start, **kw),
            self.process(element.stop, **kw),
        )

    def visit_any(self, element, **kw):
        return "%s%sANY (%s)" % (
            self.process(element.left, **kw),
            compiler.OPERATORS[element.operator],
            self.process(element.right, **kw)
        )

    def visit_all(self, element, **kw):
        return "%s%sALL (%s)" % (
            self.process(element.left, **kw),
            compiler.OPERATORS[element.operator],
            self.process(element.right, **kw)
        )

    def visit_getitem_binary(self, binary, operator, **kw):
        return "%s[%s]" % (
            self.process(binary.left, **kw),
            self.process(binary.right, **kw)
        )

    def visit_match_op_binary(self, binary, operator, **kw):
        if "postgresql_regconfig" in binary.modifiers:
            regconfig = self.render_literal_value(
                binary.modifiers['postgresql_regconfig'],
                sqltypes.STRINGTYPE)
            if regconfig:
                return "%s @@ to_tsquery(%s, %s)" % (
                    self.process(binary.left, **kw),
                    regconfig,
                    self.process(binary.right, **kw)
                )
        return "%s @@ to_tsquery(%s)" % (
            self.process(binary.left, **kw),
            self.process(binary.right, **kw)
        )

    def visit_ilike_op_binary(self, binary, operator, **kw):
        escape = binary.modifiers.get("escape", None)

        return '%s ILIKE %s' % \
            (self.process(binary.left, **kw),
             self.process(binary.right, **kw)) \
            + (
                ' ESCAPE ' +
                self.render_literal_value(escape, sqltypes.STRINGTYPE)
                if escape else ''
            )

    def visit_notilike_op_binary(self, binary, operator, **kw):
        escape = binary.modifiers.get("escape", None)
        return '%s NOT ILIKE %s' % \
            (self.process(binary.left, **kw),
             self.process(binary.right, **kw)) \
            + (
                ' ESCAPE ' +
                self.render_literal_value(escape, sqltypes.STRINGTYPE)
                if escape else ''
            )

    def render_literal_value(self, value, type_):
        value = super(PGCompiler, self).render_literal_value(value, type_)

        if self.dialect._backslash_escapes:
            value = value.replace('\\', '\\\\')
        return value

    def visit_sequence(self, seq):
        return "nextval('%s')" % self.preparer.format_sequence(seq)

    def limit_clause(self, select, **kw):
        text = ""
        if select._limit_clause is not None:
            text += " \n LIMIT " + self.process(select._limit_clause, **kw)
        if select._offset_clause is not None:
            if select._limit_clause is None:
                text += " \n LIMIT ALL"
            text += " OFFSET " + self.process(select._offset_clause, **kw)
        return text

    def format_from_hint_text(self, sqltext, table, hint, iscrud):
        if hint.upper() != 'ONLY':
            raise exc.CompileError("Unrecognized hint: %r" % hint)
        return "ONLY " + sqltext

    def get_select_precolumns(self, select, **kw):
        if select._distinct is not False:
            if select._distinct is True:
                return "DISTINCT "
            elif isinstance(select._distinct, (list, tuple)):
                return "DISTINCT ON (" + ', '.join(
                    [self.process(col) for col in select._distinct]
                ) + ") "
            else:
                return "DISTINCT ON (" + \
                    self.process(select._distinct, **kw) + ") "
        else:
            return ""

    def for_update_clause(self, select, **kw):

        if select._for_update_arg.read:
            tmp = " FOR SHARE"
        else:
            tmp = " FOR UPDATE"

        if select._for_update_arg.of:
            tables = util.OrderedSet(
                c.table if isinstance(c, expression.ColumnClause)
                else c for c in select._for_update_arg.of)
            tmp += " OF " + ", ".join(
                self.process(table, ashint=True, use_schema=False, **kw)
                for table in tables
            )

        if select._for_update_arg.nowait:
            tmp += " NOWAIT"

        return tmp

    def returning_clause(self, stmt, returning_cols):

        columns = [
            self._label_select_column(None, c, True, False, {})
            for c in expression._select_iterables(returning_cols)
        ]

        return 'RETURNING ' + ', '.join(columns)

    def visit_substring_func(self, func, **kw):
        s = self.process(func.clauses.clauses[0], **kw)
        start = self.process(func.clauses.clauses[1], **kw)
        if len(func.clauses.clauses) > 2:
            length = self.process(func.clauses.clauses[2], **kw)
            return "SUBSTRING(%s FROM %s FOR %s)" % (s, start, length)
        else:
            return "SUBSTRING(%s FROM %s)" % (s, start)


class PGDDLCompiler(compiler.DDLCompiler):

    def get_column_specification(self, column, **kwargs):

        colspec = self.preparer.format_column(column)
        impl_type = column.type.dialect_impl(self.dialect)
        if column.primary_key and \
            column is column.table._autoincrement_column and \
            (
                self.dialect.supports_smallserial or
                not isinstance(impl_type, sqltypes.SmallInteger)
            ) and (
                column.default is None or
                (
                    isinstance(column.default, schema.Sequence) and
                    column.default.optional
                )):
            if isinstance(impl_type, sqltypes.BigInteger):
                colspec += " BIGSERIAL"
            elif isinstance(impl_type, sqltypes.SmallInteger):
                colspec += " SMALLSERIAL"
            else:
                colspec += " SERIAL"
        else:
            colspec += " " + self.dialect.type_compiler.process(column.type,
                                                    type_expression=column)
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        return colspec

    def visit_create_enum_type(self, create):
        type_ = create.element

        return "CREATE TYPE %s AS ENUM (%s)" % (
            self.preparer.format_type(type_),
            ", ".join(
                self.sql_compiler.process(sql.literal(e), literal_binds=True)
                for e in type_.enums)
        )

    def visit_drop_enum_type(self, drop):
        type_ = drop.element

        return "DROP TYPE %s" % (
            self.preparer.format_type(type_)
        )

    def visit_create_index(self, create):
        preparer = self.preparer
        index = create.element
        self._verify_index_table(index)
        text = "CREATE "
        if index.unique:
            text += "UNIQUE "
        text += "INDEX "

        concurrently = index.dialect_options['postgresql']['concurrently']
        if concurrently:
            text += "CONCURRENTLY "

        text += "%s ON %s " % (
            self._prepared_index_name(index,
                                      include_schema=False),
            preparer.format_table(index.table)
        )

        using = index.dialect_options['postgresql']['using']
        if using:
            text += "USING %s " % preparer.quote(using)

        ops = index.dialect_options["postgresql"]["ops"]
        text += "(%s)" \
                % (
                    ', '.join([
                        self.sql_compiler.process(
                            expr.self_group()
                            if not isinstance(expr, expression.ColumnClause)
                            else expr,
                            include_table=False, literal_binds=True) +
                        (
                            (' ' + ops[expr.key])
                            if hasattr(expr, 'key')
                            and expr.key in ops else ''
                        )
                        for expr in index.expressions
                    ])
                )

        withclause = index.dialect_options['postgresql']['with']

        if withclause:
            text += " WITH (%s)" % (', '.join(
                ['%s = %s' % storage_parameter
                 for storage_parameter in withclause.items()]))

        whereclause = index.dialect_options["postgresql"]["where"]

        if whereclause is not None:
            where_compiled = self.sql_compiler.process(
                whereclause, include_table=False,
                literal_binds=True)
            text += " WHERE " + where_compiled
        return text

    def visit_exclude_constraint(self, constraint, **kw):
        text = ""
        if constraint.name is not None:
            text += "CONSTRAINT %s " % \
                    self.preparer.format_constraint(constraint)
        elements = []
        for expr, name, op in constraint._render_exprs:
            kw['include_table'] = False
            elements.append(
                "%s WITH %s" % (self.sql_compiler.process(expr, **kw), op)
            )
        text += "EXCLUDE USING %s (%s)" % (constraint.using,
                                           ', '.join(elements))
        if constraint.where is not None:
            text += ' WHERE (%s)' % self.sql_compiler.process(
                constraint.where,
                literal_binds=True)
        text += self.define_constraint_deferrability(constraint)
        return text

    def post_create_table(self, table):
        table_opts = []
        pg_opts = table.dialect_options['postgresql']

        inherits = pg_opts.get('inherits')
        if inherits is not None:
            if not isinstance(inherits, (list, tuple)):
                inherits = (inherits, )
            table_opts.append(
                '\n INHERITS ( ' +
                ', '.join(self.preparer.quote(name) for name in inherits) +
                ' )')

        if pg_opts['with_oids'] is True:
            table_opts.append('\n WITH OIDS')
        elif pg_opts['with_oids'] is False:
            table_opts.append('\n WITHOUT OIDS')

        if pg_opts['on_commit']:
            on_commit_options = pg_opts['on_commit'].replace("_", " ").upper()
            table_opts.append('\n ON COMMIT %s' % on_commit_options)

        if pg_opts['tablespace']:
            tablespace_name = pg_opts['tablespace']
            table_opts.append(
                '\n TABLESPACE %s' % self.preparer.quote(tablespace_name)
            )

        return ''.join(table_opts)


class PGTypeCompiler(compiler.GenericTypeCompiler):
    def visit_TSVECTOR(self, type, **kw):
        return "TSVECTOR"

    def visit_INET(self, type_, **kw):
        return "INET"

    def visit_CIDR(self, type_, **kw):
        return "CIDR"

    def visit_MACADDR(self, type_, **kw):
        return "MACADDR"

    def visit_OID(self, type_, **kw):
        return "OID"

    def visit_FLOAT(self, type_, **kw):
        if not type_.precision:
            return "FLOAT"
        else:
            return "FLOAT(%(precision)s)" % {'precision': type_.precision}

    def visit_DOUBLE_PRECISION(self, type_, **kw):
        return "DOUBLE PRECISION"

    def visit_BIGINT(self, type_, **kw):
        return "BIGINT"

    def visit_HSTORE(self, type_, **kw):
        return "HSTORE"

    def visit_JSON(self, type_, **kw):
        return "JSON"

    def visit_JSONB(self, type_, **kw):
        return "JSONB"

    def visit_INT4RANGE(self, type_, **kw):
        return "INT4RANGE"

    def visit_INT8RANGE(self, type_, **kw):
        return "INT8RANGE"

    def visit_NUMRANGE(self, type_, **kw):
        return "NUMRANGE"

    def visit_DATERANGE(self, type_, **kw):
        return "DATERANGE"

    def visit_TSRANGE(self, type_, **kw):
        return "TSRANGE"

    def visit_TSTZRANGE(self, type_, **kw):
        return "TSTZRANGE"

    def visit_datetime(self, type_, **kw):
        return self.visit_TIMESTAMP(type_, **kw)

    def visit_enum(self, type_, **kw):
        if not type_.native_enum or not self.dialect.supports_native_enum:
            return super(PGTypeCompiler, self).visit_enum(type_, **kw)
        else:
            return self.visit_ENUM(type_, **kw)

    def visit_ENUM(self, type_, **kw):
        return self.dialect.identifier_preparer.format_type(type_)

    def visit_TIMESTAMP(self, type_, **kw):
        return "TIMESTAMP%s %s" % (
            getattr(type_, 'precision', None) and "(%d)" %
            type_.precision or "",
            (type_.timezone and "WITH" or "WITHOUT") + " TIME ZONE"
        )

    def visit_TIME(self, type_, **kw):
        return "TIME%s %s" % (
            getattr(type_, 'precision', None) and "(%d)" %
            type_.precision or "",
            (type_.timezone and "WITH" or "WITHOUT") + " TIME ZONE"
        )

    def visit_INTERVAL(self, type_, **kw):
        if type_.precision is not None:
            return "INTERVAL(%d)" % type_.precision
        else:
            return "INTERVAL"

    def visit_BIT(self, type_, **kw):
        if type_.varying:
            compiled = "BIT VARYING"
            if type_.length is not None:
                compiled += "(%d)" % type_.length
        else:
            compiled = "BIT(%d)" % type_.length
        return compiled

    def visit_UUID(self, type_, **kw):
        return "UUID"

    def visit_large_binary(self, type_, **kw):
        return self.visit_BYTEA(type_, **kw)

    def visit_BYTEA(self, type_, **kw):
        return "BYTEA"

    def visit_ARRAY(self, type_, **kw):
        return self.process(type_.item_type) + ('[]' * (type_.dimensions
                                                        if type_.dimensions
                                                        is not None else 1))


class PGIdentifierPreparer(compiler.IdentifierPreparer):

    reserved_words = RESERVED_WORDS

    def _unquote_identifier(self, value):
        if value[0] == self.initial_quote:
            value = value[1:-1].\
                replace(self.escape_to_quote, self.escape_quote)
        return value

    def format_type(self, type_, use_schema=True):
        if not type_.name:
            raise exc.CompileError("Postgresql ENUM type requires a name.")

        name = self.quote(type_.name)
        if not self.omit_schema and use_schema and type_.schema is not None:
            name = self.quote_schema(type_.schema) + "." + name
        return name


class PGInspector(reflection.Inspector):

    def __init__(self, conn):
        reflection.Inspector.__init__(self, conn)

    def get_table_oid(self, table_name, schema=None):
        """Return the OID for the given table name."""

        return self.dialect.get_table_oid(self.bind, table_name, schema,
                                          info_cache=self.info_cache)

    def get_enums(self, schema=None):
        """Return a list of ENUM objects.

        Each member is a dictionary containing these fields:

            * name - name of the enum
            * schema - the schema name for the enum.
            * visible - boolean, whether or not this enum is visible
              in the default search path.
            * labels - a list of string labels that apply to the enum.

        :param schema: schema name.  If None, the default schema
         (typically 'public') is used.  May also be set to '*' to
         indicate load enums for all schemas.

        .. versionadded:: 1.0.0

        """
        schema = schema or self.default_schema_name
        return self.dialect._load_enums(self.bind, schema)

    def get_foreign_table_names(self, schema=None):
        """Return a list of FOREIGN TABLE names.

        Behavior is similar to that of :meth:`.Inspector.get_table_names`,
        except that the list is limited to those tables tha report a
        ``relkind`` value of ``f``.

        .. versionadded:: 1.0.0

        """
        schema = schema or self.default_schema_name
        return self.dialect._get_foreign_table_names(self.bind, schema)


class CreateEnumType(schema._CreateDropBase):
    __visit_name__ = "create_enum_type"


class DropEnumType(schema._CreateDropBase):
    __visit_name__ = "drop_enum_type"


class PGExecutionContext(default.DefaultExecutionContext):

    def fire_sequence(self, seq, type_):
        return self._execute_scalar((
            "select nextval('%s')" %
            self.dialect.identifier_preparer.format_sequence(seq)), type_)

    def get_insert_default(self, column):
        if column.primary_key and \
                column is column.table._autoincrement_column:
            if column.server_default and column.server_default.has_argument:

                # pre-execute passive defaults on primary key columns
                return self._execute_scalar("select %s" %
                                            column.server_default.arg,
                                            column.type)

            elif (column.default is None or
                  (column.default.is_sequence and
                   column.default.optional)):

                # execute the sequence associated with a SERIAL primary
                # key column. for non-primary-key SERIAL, the ID just
                # generates server side.

                try:
                    seq_name = column._postgresql_seq_name
                except AttributeError:
                    tab = column.table.name
                    col = column.name
                    tab = tab[0:29 + max(0, (29 - len(col)))]
                    col = col[0:29 + max(0, (29 - len(tab)))]
                    name = "%s_%s_seq" % (tab, col)
                    column._postgresql_seq_name = seq_name = name

                sch = column.table.schema
                if sch is not None:
                    exc = "select nextval('\"%s\".\"%s\"')" % \
                        (sch, seq_name)
                else:
                    exc = "select nextval('\"%s\"')" % \
                        (seq_name, )

                return self._execute_scalar(exc, column.type)

        return super(PGExecutionContext, self).get_insert_default(column)


class PGDialect(default.DefaultDialect):
    name = 'postgresql'
    supports_alter = True
    max_identifier_length = 63
    supports_sane_rowcount = True

    supports_native_enum = True
    supports_native_boolean = True
    supports_smallserial = True

    supports_sequences = True
    sequences_optional = True
    preexecute_autoincrement_sequences = True
    postfetch_lastrowid = False

    supports_default_values = True
    supports_empty_insert = False
    supports_multivalues_insert = True
    default_paramstyle = 'pyformat'
    ischema_names = ischema_names
    colspecs = colspecs

    statement_compiler = PGCompiler
    ddl_compiler = PGDDLCompiler
    type_compiler = PGTypeCompiler
    preparer = PGIdentifierPreparer
    execution_ctx_cls = PGExecutionContext
    inspector = PGInspector
    isolation_level = None

    construct_arguments = [
        (schema.Index, {
            "using": False,
            "where": None,
            "ops": {},
            "concurrently": False,
            "with": {}
        }),
        (schema.Table, {
            "ignore_search_path": False,
            "tablespace": None,
            "with_oids": None,
            "on_commit": None,
            "inherits": None
        })
    ]

    reflection_options = ('postgresql_ignore_search_path', )

    _backslash_escapes = True

    def __init__(self, isolation_level=None, json_serializer=None,
                 json_deserializer=None, **kwargs):
        default.DefaultDialect.__init__(self, **kwargs)
        self.isolation_level = isolation_level
        self._json_deserializer = json_deserializer
        self._json_serializer = json_serializer

    def initialize(self, connection):
        super(PGDialect, self).initialize(connection)
        self.implicit_returning = self.server_version_info > (8, 2) and \
            self.__dict__.get('implicit_returning', True)
        self.supports_native_enum = self.server_version_info >= (8, 3)
        if not self.supports_native_enum:
            self.colspecs = self.colspecs.copy()
            # pop base Enum type
            self.colspecs.pop(sqltypes.Enum, None)
            # psycopg2, others may have placed ENUM here as well
            self.colspecs.pop(ENUM, None)

        # http://www.postgresql.org/docs/9.3/static/release-9-2.html#AEN116689
        self.supports_smallserial = self.server_version_info >= (9, 2)

        self._backslash_escapes = self.server_version_info < (8, 2) or \
            connection.scalar(
            "show standard_conforming_strings"
        ) == 'off'

    def on_connect(self):
        if self.isolation_level is not None:
            def connect(conn):
                self.set_isolation_level(conn, self.isolation_level)
            return connect
        else:
            return None

    _isolation_lookup = set(['SERIALIZABLE', 'READ UNCOMMITTED',
                             'READ COMMITTED', 'REPEATABLE READ'])

    def set_isolation_level(self, connection, level):
        level = level.replace('_', ' ')
        if level not in self._isolation_lookup:
            raise exc.ArgumentError(
                "Invalid value '%s' for isolation_level. "
                "Valid isolation levels for %s are %s" %
                (level, self.name, ", ".join(self._isolation_lookup))
            )
        cursor = connection.cursor()
        cursor.execute(
            "SET SESSION CHARACTERISTICS AS TRANSACTION "
            "ISOLATION LEVEL %s" % level)
        cursor.execute("COMMIT")
        cursor.close()

    def get_isolation_level(self, connection):
        cursor = connection.cursor()
        cursor.execute('show transaction isolation level')
        val = cursor.fetchone()[0]
        cursor.close()
        return val.upper()

    def do_begin_twophase(self, connection, xid):
        self.do_begin(connection.connection)

    def do_prepare_twophase(self, connection, xid):
        connection.execute("PREPARE TRANSACTION '%s'" % xid)

    def do_rollback_twophase(self, connection, xid,
                             is_prepared=True, recover=False):
        if is_prepared:
            if recover:
                # FIXME: ugly hack to get out of transaction
                # context when committing recoverable transactions
                # Must find out a way how to make the dbapi not
                # open a transaction.
                connection.execute("ROLLBACK")
            connection.execute("ROLLBACK PREPARED '%s'" % xid)
            connection.execute("BEGIN")
            self.do_rollback(connection.connection)
        else:
            self.do_rollback(connection.connection)

    def do_commit_twophase(self, connection, xid,
                           is_prepared=True, recover=False):
        if is_prepared:
            if recover:
                connection.execute("ROLLBACK")
            connection.execute("COMMIT PREPARED '%s'" % xid)
            connection.execute("BEGIN")
            self.do_rollback(connection.connection)
        else:
            self.do_commit(connection.connection)

    def do_recover_twophase(self, connection):
        resultset = connection.execute(
            sql.text("SELECT gid FROM pg_prepared_xacts"))
        return [row[0] for row in resultset]

    def _get_default_schema_name(self, connection):
        return connection.scalar("select current_schema()")

    def has_schema(self, connection, schema):
        query = ("select nspname from pg_namespace "
                 "where lower(nspname)=:schema")
        cursor = connection.execute(
            sql.text(
                query,
                bindparams=[
                    sql.bindparam(
                        'schema', util.text_type(schema.lower()),
                        type_=sqltypes.Unicode)]
            )
        )

        return bool(cursor.first())

    def has_table(self, connection, table_name, schema=None):
        # seems like case gets folded in pg_class...
        if schema is None:
            cursor = connection.execute(
                sql.text(
                    "select relname from pg_class c join pg_namespace n on "
                    "n.oid=c.relnamespace where "
                    "pg_catalog.pg_table_is_visible(c.oid) "
                    "and relname=:name",
                    bindparams=[
                        sql.bindparam('name', util.text_type(table_name),
                                      type_=sqltypes.Unicode)]
                )
            )
        else:
            cursor = connection.execute(
                sql.text(
                    "select relname from pg_class c join pg_namespace n on "
                    "n.oid=c.relnamespace where n.nspname=:schema and "
                    "relname=:name",
                    bindparams=[
                        sql.bindparam('name',
                                      util.text_type(table_name),
                                      type_=sqltypes.Unicode),
                        sql.bindparam('schema',
                                      util.text_type(schema),
                                      type_=sqltypes.Unicode)]
                )
            )
        return bool(cursor.first())

    def has_sequence(self, connection, sequence_name, schema=None):
        if schema is None:
            cursor = connection.execute(
                sql.text(
                    "SELECT relname FROM pg_class c join pg_namespace n on "
                    "n.oid=c.relnamespace where relkind='S' and "
                    "n.nspname=current_schema() "
                    "and relname=:name",
                    bindparams=[
                        sql.bindparam('name', util.text_type(sequence_name),
                                      type_=sqltypes.Unicode)
                    ]
                )
            )
        else:
            cursor = connection.execute(
                sql.text(
                    "SELECT relname FROM pg_class c join pg_namespace n on "
                    "n.oid=c.relnamespace where relkind='S' and "
                    "n.nspname=:schema and relname=:name",
                    bindparams=[
                        sql.bindparam('name', util.text_type(sequence_name),
                                      type_=sqltypes.Unicode),
                        sql.bindparam('schema',
                                      util.text_type(schema),
                                      type_=sqltypes.Unicode)
                    ]
                )
            )

        return bool(cursor.first())

    def has_type(self, connection, type_name, schema=None):
        if schema is not None:
            query = """
            SELECT EXISTS (
                SELECT * FROM pg_catalog.pg_type t, pg_catalog.pg_namespace n
                WHERE t.typnamespace = n.oid
                AND t.typname = :typname
                AND n.nspname = :nspname
                )
                """
            query = sql.text(query)
        else:
            query = """
            SELECT EXISTS (
                SELECT * FROM pg_catalog.pg_type t
                WHERE t.typname = :typname
                AND pg_type_is_visible(t.oid)
                )
                """
            query = sql.text(query)
        query = query.bindparams(
            sql.bindparam('typname',
                          util.text_type(type_name), type_=sqltypes.Unicode),
        )
        if schema is not None:
            query = query.bindparams(
                sql.bindparam('nspname',
                              util.text_type(schema), type_=sqltypes.Unicode),
            )
        cursor = connection.execute(query)
        return bool(cursor.scalar())

    def _get_server_version_info(self, connection):
        v = connection.execute("select version()").scalar()
        m = re.match(
            '.*(?:PostgreSQL|EnterpriseDB) '
            '(\d+)\.(\d+)(?:\.(\d+))?(?:\.\d+)?(?:devel)?',
            v)
        if not m:
            raise AssertionError(
                "Could not determine version from string '%s'" % v)
        return tuple([int(x) for x in m.group(1, 2, 3) if x is not None])

    @reflection.cache
    def get_table_oid(self, connection, table_name, schema=None, **kw):
        """Fetch the oid for schema.table_name.

        Several reflection methods require the table oid.  The idea for using
        this method is that it can be fetched one time and cached for
        subsequent calls.

        """
        table_oid = None
        if schema is not None:
            schema_where_clause = "n.nspname = :schema"
        else:
            schema_where_clause = "pg_catalog.pg_table_is_visible(c.oid)"
        query = """
            SELECT c.oid
            FROM pg_catalog.pg_class c
            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE (%s)
            AND c.relname = :table_name AND c.relkind in ('r', 'v', 'm', 'f')
        """ % schema_where_clause
        # Since we're binding to unicode, table_name and schema_name must be
        # unicode.
        table_name = util.text_type(table_name)
        if schema is not None:
            schema = util.text_type(schema)
        s = sql.text(query).bindparams(table_name=sqltypes.Unicode)
        s = s.columns(oid=sqltypes.Integer)
        if schema:
            s = s.bindparams(sql.bindparam('schema', type_=sqltypes.Unicode))
        c = connection.execute(s, table_name=table_name, schema=schema)
        table_oid = c.scalar()
        if table_oid is None:
            raise exc.NoSuchTableError(table_name)
        return table_oid

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        s = """
        SELECT nspname
        FROM pg_namespace
        ORDER BY nspname
        """
        rp = connection.execute(s)
        # what about system tables?

        if util.py2k:
            schema_names = [row[0].decode(self.encoding) for row in rp
                            if not row[0].startswith('pg_')]
        else:
            schema_names = [row[0] for row in rp
                            if not row[0].startswith('pg_')]
        return schema_names

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        if schema is not None:
            current_schema = schema
        else:
            current_schema = self.default_schema_name

        result = connection.execute(
            sql.text("SELECT relname FROM pg_class c "
                     "WHERE relkind = 'r' "
                     "AND '%s' = (select nspname from pg_namespace n "
                     "where n.oid = c.relnamespace) " %
                     current_schema,
                     typemap={'relname': sqltypes.Unicode}
                     )
        )
        return [row[0] for row in result]

    @reflection.cache
    def _get_foreign_table_names(self, connection, schema=None, **kw):
        if schema is not None:
            current_schema = schema
        else:
            current_schema = self.default_schema_name

        result = connection.execute(
            sql.text("SELECT relname FROM pg_class c "
                     "WHERE relkind = 'f' "
                     "AND '%s' = (select nspname from pg_namespace n "
                     "where n.oid = c.relnamespace) " %
                     current_schema,
                     typemap={'relname': sqltypes.Unicode}
                     )
        )
        return [row[0] for row in result]

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        if schema is not None:
            current_schema = schema
        else:
            current_schema = self.default_schema_name
        s = """
        SELECT relname
        FROM pg_class c
        WHERE relkind IN ('m', 'v')
          AND '%(schema)s' = (select nspname from pg_namespace n
          where n.oid = c.relnamespace)
        """ % dict(schema=current_schema)

        if util.py2k:
            view_names = [row[0].decode(self.encoding)
                          for row in connection.execute(s)]
        else:
            view_names = [row[0] for row in connection.execute(s)]
        return view_names

    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        if schema is not None:
            current_schema = schema
        else:
            current_schema = self.default_schema_name
        s = """
        SELECT definition FROM pg_views
        WHERE schemaname = :schema
        AND viewname = :view_name
        """
        rp = connection.execute(sql.text(s),
                                view_name=view_name, schema=current_schema)
        if rp:
            if util.py2k:
                view_def = rp.scalar().decode(self.encoding)
            else:
                view_def = rp.scalar()
            return view_def

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):

        table_oid = self.get_table_oid(connection, table_name, schema,
                                       info_cache=kw.get('info_cache'))
        SQL_COLS = """
            SELECT a.attname,
              pg_catalog.format_type(a.atttypid, a.atttypmod),
              (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid)
                FROM pg_catalog.pg_attrdef d
               WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum
               AND a.atthasdef)
              AS DEFAULT,
              a.attnotnull, a.attnum, a.attrelid as table_oid
            FROM pg_catalog.pg_attribute a
            WHERE a.attrelid = :table_oid
            AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
        """
        s = sql.text(SQL_COLS,
                     bindparams=[
                         sql.bindparam('table_oid', type_=sqltypes.Integer)],
                     typemap={
                         'attname': sqltypes.Unicode,
                         'default': sqltypes.Unicode}
                     )
        c = connection.execute(s, table_oid=table_oid)
        rows = c.fetchall()
        domains = self._load_domains(connection)
        enums = dict(
            (
                "%s.%s" % (rec['schema'], rec['name'])
                if not rec['visible'] else rec['name'], rec) for rec in
            self._load_enums(connection, schema='*')
        )

        # format columns
        columns = []
        for name, format_type, default, notnull, attnum, table_oid in rows:
            column_info = self._get_column_info(
                name, format_type, default, notnull, domains, enums, schema)
            columns.append(column_info)
        return columns

    def _get_column_info(self, name, format_type, default,
                         notnull, domains, enums, schema):
        # strip (*) from character varying(5), timestamp(5)
        # with time zone, geometry(POLYGON), etc.
        attype = re.sub(r'\(.*\)', '', format_type)

        # strip '[]' from integer[], etc.
        attype = re.sub(r'\[\]', '', attype)

        nullable = not notnull
        is_array = format_type.endswith('[]')
        charlen = re.search('\(([\d,]+)\)', format_type)
        if charlen:
            charlen = charlen.group(1)
        args = re.search('\((.*)\)', format_type)
        if args and args.group(1):
            args = tuple(re.split('\s*,\s*', args.group(1)))
        else:
            args = ()
        kwargs = {}

        if attype == 'numeric':
            if charlen:
                prec, scale = charlen.split(',')
                args = (int(prec), int(scale))
            else:
                args = ()
        elif attype == 'double precision':
            args = (53, )
        elif attype == 'integer':
            args = ()
        elif attype in ('timestamp with time zone',
                        'time with time zone'):
            kwargs['timezone'] = True
            if charlen:
                kwargs['precision'] = int(charlen)
            args = ()
        elif attype in ('timestamp without time zone',
                        'time without time zone', 'time'):
            kwargs['timezone'] = False
            if charlen:
                kwargs['precision'] = int(charlen)
            args = ()
        elif attype == 'bit varying':
            kwargs['varying'] = True
            if charlen:
                args = (int(charlen),)
            else:
                args = ()
        elif attype in ('interval', 'interval year to month',
                        'interval day to second'):
            if charlen:
                kwargs['precision'] = int(charlen)
            args = ()
        elif charlen:
            args = (int(charlen),)

        while True:
            if attype in self.ischema_names:
                coltype = self.ischema_names[attype]
                break
            elif attype in enums:
                enum = enums[attype]
                coltype = ENUM
                kwargs['name'] = enum['name']
                if not enum['visible']:
                    kwargs['schema'] = enum['schema']
                args = tuple(enum['labels'])
                break
            elif attype in domains:
                domain = domains[attype]
                attype = domain['attype']
                # A table can't override whether the domain is nullable.
                nullable = domain['nullable']
                if domain['default'] and not default:
                    # It can, however, override the default
                    # value, but can't set it to null.
                    default = domain['default']
                continue
            else:
                coltype = None
                break

        if coltype:
            coltype = coltype(*args, **kwargs)
            if is_array:
                coltype = ARRAY(coltype)
        else:
            util.warn("Did not recognize type '%s' of column '%s'" %
                      (attype, name))
            coltype = sqltypes.NULLTYPE
        # adjust the default value
        autoincrement = False
        if default is not None:
            match = re.search(r"""(nextval\(')([^']+)('.*$)""", default)
            if match is not None:
                autoincrement = True
                # the default is related to a Sequence
                sch = schema
                if '.' not in match.group(2) and sch is not None:
                    # unconditionally quote the schema name.  this could
                    # later be enhanced to obey quoting rules /
                    # "quote schema"
                    default = match.group(1) + \
                        ('"%s"' % sch) + '.' + \
                        match.group(2) + match.group(3)

        column_info = dict(name=name, type=coltype, nullable=nullable,
                           default=default, autoincrement=autoincrement)
        return column_info

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        table_oid = self.get_table_oid(connection, table_name, schema,
                                       info_cache=kw.get('info_cache'))

        if self.server_version_info < (8, 4):
            PK_SQL = """
                SELECT a.attname
                FROM
                    pg_class t
                    join pg_index ix on t.oid = ix.indrelid
                    join pg_attribute a
                        on t.oid=a.attrelid AND %s
                 WHERE
                  t.oid = :table_oid and ix.indisprimary = 't'
                ORDER BY a.attnum
            """ % self._pg_index_any("a.attnum", "ix.indkey")

        else:
            # unnest() and generate_subscripts() both introduced in
            # version 8.4
            PK_SQL = """
                SELECT a.attname
                FROM pg_attribute a JOIN (
                    SELECT unnest(ix.indkey) attnum,
                           generate_subscripts(ix.indkey, 1) ord
                    FROM pg_index ix
                    WHERE ix.indrelid = :table_oid AND ix.indisprimary
                    ) k ON a.attnum=k.attnum
                WHERE a.attrelid = :table_oid
                ORDER BY k.ord
            """
        t = sql.text(PK_SQL, typemap={'attname': sqltypes.Unicode})
        c = connection.execute(t, table_oid=table_oid)
        cols = [r[0] for r in c.fetchall()]

        PK_CONS_SQL = """
        SELECT conname
           FROM  pg_catalog.pg_constraint r
           WHERE r.conrelid = :table_oid AND r.contype = 'p'
           ORDER BY 1
        """
        t = sql.text(PK_CONS_SQL, typemap={'conname': sqltypes.Unicode})
        c = connection.execute(t, table_oid=table_oid)
        name = c.scalar()

        return {'constrained_columns': cols, 'name': name}

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None,
                         postgresql_ignore_search_path=False, **kw):
        preparer = self.identifier_preparer
        table_oid = self.get_table_oid(connection, table_name, schema,
                                       info_cache=kw.get('info_cache'))

        FK_SQL = """
          SELECT r.conname,
                pg_catalog.pg_get_constraintdef(r.oid, true) as condef,
                n.nspname as conschema
          FROM  pg_catalog.pg_constraint r,
                pg_namespace n,
                pg_class c

          WHERE r.conrelid = :table AND
                r.contype = 'f' AND
                c.oid = confrelid AND
                n.oid = c.relnamespace
          ORDER BY 1
        """
        # http://www.postgresql.org/docs/9.0/static/sql-createtable.html
        FK_REGEX = re.compile(
            r'FOREIGN KEY \((.*?)\) REFERENCES (?:(.*?)\.)?(.*?)\((.*?)\)'
            r'[\s]?(MATCH (FULL|PARTIAL|SIMPLE)+)?'
            r'[\s]?(ON UPDATE '
            r'(CASCADE|RESTRICT|NO ACTION|SET NULL|SET DEFAULT)+)?'
            r'[\s]?(ON DELETE '
            r'(CASCADE|RESTRICT|NO ACTION|SET NULL|SET DEFAULT)+)?'
            r'[\s]?(DEFERRABLE|NOT DEFERRABLE)?'
            r'[\s]?(INITIALLY (DEFERRED|IMMEDIATE)+)?'
        )

        t = sql.text(FK_SQL, typemap={
            'conname': sqltypes.Unicode,
            'condef': sqltypes.Unicode})
        c = connection.execute(t, table=table_oid)
        fkeys = []
        for conname, condef, conschema in c.fetchall():
            m = re.search(FK_REGEX, condef).groups()

            constrained_columns, referred_schema, \
                referred_table, referred_columns, \
                _, match, _, onupdate, _, ondelete, \
                deferrable, _, initially = m

            if deferrable is not None:
                deferrable = True if deferrable == 'DEFERRABLE' else False
            constrained_columns = [preparer._unquote_identifier(x)
                                   for x in re.split(
                                       r'\s*,\s*', constrained_columns)]

            if postgresql_ignore_search_path:
                # when ignoring search path, we use the actual schema
                # provided it isn't the "default" schema
                if conschema != self.default_schema_name:
                    referred_schema = conschema
                else:
                    referred_schema = schema
            elif referred_schema:
                # referred_schema is the schema that we regexp'ed from
                # pg_get_constraintdef().  If the schema is in the search
                # path, pg_get_constraintdef() will give us None.
                referred_schema = \
                    preparer._unquote_identifier(referred_schema)
            elif schema is not None and schema == conschema:
                # If the actual schema matches the schema of the table
                # we're reflecting, then we will use that.
                referred_schema = schema

            referred_table = preparer._unquote_identifier(referred_table)
            referred_columns = [preparer._unquote_identifier(x)
                                for x in
                                re.split(r'\s*,\s', referred_columns)]
            fkey_d = {
                'name': conname,
                'constrained_columns': constrained_columns,
                'referred_schema': referred_schema,
                'referred_table': referred_table,
                'referred_columns': referred_columns,
                'options': {
                    'onupdate': onupdate,
                    'ondelete': ondelete,
                    'deferrable': deferrable,
                    'initially': initially,
                    'match': match
                }
            }
            fkeys.append(fkey_d)
        return fkeys

    def _pg_index_any(self, col, compare_to):
        if self.server_version_info < (8, 1):
            # http://www.postgresql.org/message-id/10279.1124395722@sss.pgh.pa.us
            # "In CVS tip you could replace this with "attnum = ANY (indkey)".
            # Unfortunately, most array support doesn't work on int2vector in
            # pre-8.1 releases, so I think you're kinda stuck with the above
            # for now.
            # regards, tom lane"
            return "(%s)" % " OR ".join(
                "%s[%d] = %s" % (compare_to, ind, col)
                for ind in range(0, 10)
            )
        else:
            return "%s = ANY(%s)" % (col, compare_to)

    @reflection.cache
    def get_indexes(self, connection, table_name, schema, **kw):
        table_oid = self.get_table_oid(connection, table_name, schema,
                                       info_cache=kw.get('info_cache'))

        # cast indkey as varchar since it's an int2vector,
        # returned as a list by some drivers such as pypostgresql

        if self.server_version_info < (8, 5):
            IDX_SQL = """
              SELECT
                  i.relname as relname,
                  ix.indisunique, ix.indexprs, ix.indpred,
                  a.attname, a.attnum, NULL, ix.indkey%s,
                  %s, am.amname
              FROM
                  pg_class t
                        join pg_index ix on t.oid = ix.indrelid
                        join pg_class i on i.oid = ix.indexrelid
                        left outer join
                            pg_attribute a
                            on t.oid = a.attrelid and %s
                        left outer join
                            pg_am am
                            on i.relam = am.oid
              WHERE
                  t.relkind IN ('r', 'v', 'f', 'm')
                  and t.oid = :table_oid
                  and ix.indisprimary = 'f'
              ORDER BY
                  t.relname,
                  i.relname
            """ % (
                # version 8.3 here was based on observing the
                # cast does not work in PG 8.2.4, does work in 8.3.0.
                # nothing in PG changelogs regarding this.
                "::varchar" if self.server_version_info >= (8, 3) else "",
                "i.reloptions" if self.server_version_info >= (8, 2)
                else "NULL",
                self._pg_index_any("a.attnum", "ix.indkey")
            )
        else:
            IDX_SQL = """
              SELECT
                  i.relname as relname,
                  ix.indisunique, ix.indexprs, ix.indpred,
                  a.attname, a.attnum, c.conrelid, ix.indkey::varchar,
                  i.reloptions, am.amname
              FROM
                  pg_class t
                        join pg_index ix on t.oid = ix.indrelid
                        join pg_class i on i.oid = ix.indexrelid
                        left outer join
                            pg_attribute a
                            on t.oid = a.attrelid and a.attnum = ANY(ix.indkey)
                        left outer join
                            pg_constraint c
                            on (ix.indrelid = c.conrelid and
                                ix.indexrelid = c.conindid and
                                c.contype in ('p', 'u', 'x'))
                        left outer join
                            pg_am am
                            on i.relam = am.oid
              WHERE
                  t.relkind IN ('r', 'v', 'f', 'm')
                  and t.oid = :table_oid
                  and ix.indisprimary = 'f'
              ORDER BY
                  t.relname,
                  i.relname
            """

        t = sql.text(IDX_SQL, typemap={'attname': sqltypes.Unicode})
        c = connection.execute(t, table_oid=table_oid)

        indexes = defaultdict(lambda: defaultdict(dict))

        sv_idx_name = None
        for row in c.fetchall():
            (idx_name, unique, expr, prd, col,
             col_num, conrelid, idx_key, options, amname) = row

            if expr:
                if idx_name != sv_idx_name:
                    util.warn(
                        "Skipped unsupported reflection of "
                        "expression-based index %s"
                        % idx_name)
                sv_idx_name = idx_name
                continue

            if prd and not idx_name == sv_idx_name:
                util.warn(
                    "Predicate of partial index %s ignored during reflection"
                    % idx_name)
                sv_idx_name = idx_name

            has_idx = idx_name in indexes
            index = indexes[idx_name]
            if col is not None:
                index['cols'][col_num] = col
            if not has_idx:
                index['key'] = [int(k.strip()) for k in idx_key.split()]
                index['unique'] = unique
                if conrelid is not None:
                    index['duplicates_constraint'] = idx_name
                if options:
                    index['options'] = dict(
                        [option.split("=") for option in options])

                # it *might* be nice to include that this is 'btree' in the
                # reflection info.  But we don't want an Index object
                # to have a ``postgresql_using`` in it that is just the
                # default, so for the moment leaving this out.
                if amname and amname != 'btree':
                    index['amname'] = amname

        result = []
        for name, idx in indexes.items():
            entry = {
                'name': name,
                'unique': idx['unique'],
                'column_names': [idx['cols'][i] for i in idx['key']]
            }
            if 'duplicates_constraint' in idx:
                entry['duplicates_constraint'] = idx['duplicates_constraint']
            if 'options' in idx:
                entry.setdefault(
                    'dialect_options', {})["postgresql_with"] = idx['options']
            if 'amname' in idx:
                entry.setdefault(
                    'dialect_options', {})["postgresql_using"] = idx['amname']
            result.append(entry)
        return result

    @reflection.cache
    def get_unique_constraints(self, connection, table_name,
                               schema=None, **kw):
        table_oid = self.get_table_oid(connection, table_name, schema,
                                       info_cache=kw.get('info_cache'))

        UNIQUE_SQL = """
            SELECT
                cons.conname as name,
                cons.conkey as key,
                a.attnum as col_num,
                a.attname as col_name
            FROM
                pg_catalog.pg_constraint cons
                join pg_attribute a
                  on cons.conrelid = a.attrelid AND
                    a.attnum = ANY(cons.conkey)
            WHERE
                cons.conrelid = :table_oid AND
                cons.contype = 'u'
        """

        t = sql.text(UNIQUE_SQL, typemap={'col_name': sqltypes.Unicode})
        c = connection.execute(t, table_oid=table_oid)

        uniques = defaultdict(lambda: defaultdict(dict))
        for row in c.fetchall():
            uc = uniques[row.name]
            uc["key"] = row.key
            uc["cols"][row.col_num] = row.col_name

        return [
            {'name': name,
             'column_names': [uc["cols"][i] for i in uc["key"]]}
            for name, uc in uniques.items()
        ]

    def _load_enums(self, connection, schema=None):
        schema = schema or self.default_schema_name
        if not self.supports_native_enum:
            return {}

        # Load data types for enums:
        SQL_ENUMS = """
            SELECT t.typname as "name",
               -- no enum defaults in 8.4 at least
               -- t.typdefault as "default",
               pg_catalog.pg_type_is_visible(t.oid) as "visible",
               n.nspname as "schema",
               e.enumlabel as "label"
            FROM pg_catalog.pg_type t
                 LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                 LEFT JOIN pg_catalog.pg_enum e ON t.oid = e.enumtypid
            WHERE t.typtype = 'e'
        """

        if schema != '*':
            SQL_ENUMS += "AND n.nspname = :schema "

        # e.oid gives us label order within an enum
        SQL_ENUMS += 'ORDER BY "schema", "name", e.oid'

        s = sql.text(SQL_ENUMS, typemap={
            'attname': sqltypes.Unicode,
            'label': sqltypes.Unicode})

        if schema != '*':
            s = s.bindparams(schema=schema)

        c = connection.execute(s)

        enums = []
        enum_by_name = {}
        for enum in c.fetchall():
            key = (enum['schema'], enum['name'])
            if key in enum_by_name:
                enum_by_name[key]['labels'].append(enum['label'])
            else:
                enum_by_name[key] = enum_rec = {
                    'name': enum['name'],
                    'schema': enum['schema'],
                    'visible': enum['visible'],
                    'labels': [enum['label']],
                }
                enums.append(enum_rec)

        return enums

    def _load_domains(self, connection):
        # Load data types for domains:
        SQL_DOMAINS = """
            SELECT t.typname as "name",
               pg_catalog.format_type(t.typbasetype, t.typtypmod) as "attype",
               not t.typnotnull as "nullable",
               t.typdefault as "default",
               pg_catalog.pg_type_is_visible(t.oid) as "visible",
               n.nspname as "schema"
            FROM pg_catalog.pg_type t
               LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
            WHERE t.typtype = 'd'
        """

        s = sql.text(SQL_DOMAINS, typemap={'attname': sqltypes.Unicode})
        c = connection.execute(s)

        domains = {}
        for domain in c.fetchall():
            # strip (30) from character varying(30)
            attype = re.search('([^\(]+)', domain['attype']).group(1)
            if domain['visible']:
                # 'visible' just means whether or not the domain is in a
                # schema that's on the search path -- or not overridden by
                # a schema with higher precedence. If it's not visible,
                # it will be prefixed with the schema-name when it's used.
                name = domain['name']
            else:
                name = "%s.%s" % (domain['schema'], domain['name'])

            domains[name] = {
                'attype': attype,
                'nullable': domain['nullable'],
                'default': domain['default']
            }

        return domains
