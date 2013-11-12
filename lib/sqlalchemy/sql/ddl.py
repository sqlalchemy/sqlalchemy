# sql/ddl.py
# Copyright (C) 2009-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
"""
Provides the hierarchy of DDL-defining schema items as well as routines
to invoke them for a create/drop call.

"""

from .. import util
from .elements import ClauseElement
from .visitors import traverse
from .base import Executable, _generative, SchemaVisitor, _bind_or_error
from ..util import topological
from .. import event
from .. import exc

class _DDLCompiles(ClauseElement):
    def _compiler(self, dialect, **kw):
        """Return a compiler appropriate for this ClauseElement, given a
        Dialect."""

        return dialect.ddl_compiler(dialect, self, **kw)


class DDLElement(Executable, _DDLCompiles):
    """Base class for DDL expression constructs.

    This class is the base for the general purpose :class:`.DDL` class,
    as well as the various create/drop clause constructs such as
    :class:`.CreateTable`, :class:`.DropTable`, :class:`.AddConstraint`,
    etc.

    :class:`.DDLElement` integrates closely with SQLAlchemy events,
    introduced in :ref:`event_toplevel`.  An instance of one is
    itself an event receiving callable::

        event.listen(
            users,
            'after_create',
            AddConstraint(constraint).execute_if(dialect='postgresql')
        )

    .. seealso::

        :class:`.DDL`

        :class:`.DDLEvents`

        :ref:`event_toplevel`

        :ref:`schema_ddl_sequences`

    """

    _execution_options = Executable.\
                            _execution_options.union({'autocommit': True})

    target = None
    on = None
    dialect = None
    callable_ = None

    def _execute_on_connection(self, connection, multiparams, params):
        return connection._execute_ddl(self, multiparams, params)

    def execute(self, bind=None, target=None):
        """Execute this DDL immediately.

        Executes the DDL statement in isolation using the supplied
        :class:`.Connectable` or
        :class:`.Connectable` assigned to the ``.bind``
        property, if not supplied. If the DDL has a conditional ``on``
        criteria, it will be invoked with None as the event.

        :param bind:
          Optional, an ``Engine`` or ``Connection``. If not supplied, a valid
          :class:`.Connectable` must be present in the
          ``.bind`` property.

        :param target:
          Optional, defaults to None.  The target SchemaItem for the
          execute call.  Will be passed to the ``on`` callable if any,
          and may also provide string expansion data for the
          statement. See ``execute_at`` for more information.

        """

        if bind is None:
            bind = _bind_or_error(self)

        if self._should_execute(target, bind):
            return bind.execute(self.against(target))
        else:
            bind.engine.logger.info(
                        "DDL execution skipped, criteria not met.")

    @util.deprecated("0.7", "See :class:`.DDLEvents`, as well as "
        ":meth:`.DDLElement.execute_if`.")
    def execute_at(self, event_name, target):
        """Link execution of this DDL to the DDL lifecycle of a SchemaItem.

        Links this ``DDLElement`` to a ``Table`` or ``MetaData`` instance,
        executing it when that schema item is created or dropped. The DDL
        statement will be executed using the same Connection and transactional
        context as the Table create/drop itself. The ``.bind`` property of
        this statement is ignored.

        :param event:
          One of the events defined in the schema item's ``.ddl_events``;
          e.g. 'before-create', 'after-create', 'before-drop' or 'after-drop'

        :param target:
          The Table or MetaData instance for which this DDLElement will
          be associated with.

        A DDLElement instance can be linked to any number of schema items.

        ``execute_at`` builds on the ``append_ddl_listener`` interface of
        :class:`.MetaData` and :class:`.Table` objects.

        Caveat: Creating or dropping a Table in isolation will also trigger
        any DDL set to ``execute_at`` that Table's MetaData.  This may change
        in a future release.

        """

        def call_event(target, connection, **kw):
            if self._should_execute_deprecated(event_name,
                                    target, connection, **kw):
                return connection.execute(self.against(target))

        event.listen(target, "" + event_name.replace('-', '_'), call_event)

    @_generative
    def against(self, target):
        """Return a copy of this DDL against a specific schema item."""

        self.target = target

    @_generative
    def execute_if(self, dialect=None, callable_=None, state=None):
        """Return a callable that will execute this
        DDLElement conditionally.

        Used to provide a wrapper for event listening::

            event.listen(
                        metadata,
                        'before_create',
                        DDL("my_ddl").execute_if(dialect='postgresql')
                    )

        :param dialect: May be a string, tuple or a callable
          predicate.  If a string, it will be compared to the name of the
          executing database dialect::

            DDL('something').execute_if(dialect='postgresql')

          If a tuple, specifies multiple dialect names::

            DDL('something').execute_if(dialect=('postgresql', 'mysql'))

        :param callable_: A callable, which will be invoked with
          four positional arguments as well as optional keyword
          arguments:

            :ddl:
              This DDL element.

            :target:
              The :class:`.Table` or :class:`.MetaData` object which is the
              target of this event. May be None if the DDL is executed
              explicitly.

            :bind:
              The :class:`.Connection` being used for DDL execution

            :tables:
              Optional keyword argument - a list of Table objects which are to
              be created/ dropped within a MetaData.create_all() or drop_all()
              method call.

            :state:
              Optional keyword argument - will be the ``state`` argument
              passed to this function.

            :checkfirst:
             Keyword argument, will be True if the 'checkfirst' flag was
             set during the call to ``create()``, ``create_all()``,
             ``drop()``, ``drop_all()``.

          If the callable returns a true value, the DDL statement will be
          executed.

        :param state: any value which will be passed to the callable\_
          as the ``state`` keyword argument.

        .. seealso::

            :class:`.DDLEvents`

            :ref:`event_toplevel`

        """
        self.dialect = dialect
        self.callable_ = callable_
        self.state = state

    def _should_execute(self, target, bind, **kw):
        if self.on is not None and \
            not self._should_execute_deprecated(None, target, bind, **kw):
            return False

        if isinstance(self.dialect, util.string_types):
            if self.dialect != bind.engine.name:
                return False
        elif isinstance(self.dialect, (tuple, list, set)):
            if bind.engine.name not in self.dialect:
                return False
        if self.callable_ is not None and \
            not self.callable_(self, target, bind, state=self.state, **kw):
            return False

        return True

    def _should_execute_deprecated(self, event, target, bind, **kw):
        if self.on is None:
            return True
        elif isinstance(self.on, util.string_types):
            return self.on == bind.engine.name
        elif isinstance(self.on, (tuple, list, set)):
            return bind.engine.name in self.on
        else:
            return self.on(self, event, target, bind, **kw)

    def __call__(self, target, bind, **kw):
        """Execute the DDL as a ddl_listener."""

        if self._should_execute(target, bind, **kw):
            return bind.execute(self.against(target))

    def _check_ddl_on(self, on):
        if (on is not None and
            (not isinstance(on, util.string_types + (tuple, list, set)) and
                    not util.callable(on))):
            raise exc.ArgumentError(
                "Expected the name of a database dialect, a tuple "
                "of names, or a callable for "
                "'on' criteria, got type '%s'." % type(on).__name__)

    def bind(self):
        if self._bind:
            return self._bind

    def _set_bind(self, bind):
        self._bind = bind
    bind = property(bind, _set_bind)

    def _generate(self):
        s = self.__class__.__new__(self.__class__)
        s.__dict__ = self.__dict__.copy()
        return s


class DDL(DDLElement):
    """A literal DDL statement.

    Specifies literal SQL DDL to be executed by the database.  DDL objects
    function as DDL event listeners, and can be subscribed to those events
    listed in :class:`.DDLEvents`, using either :class:`.Table` or
    :class:`.MetaData` objects as targets.   Basic templating support allows
    a single DDL instance to handle repetitive tasks for multiple tables.

    Examples::

      from sqlalchemy import event, DDL

      tbl = Table('users', metadata, Column('uid', Integer))
      event.listen(tbl, 'before_create', DDL('DROP TRIGGER users_trigger'))

      spow = DDL('ALTER TABLE %(table)s SET secretpowers TRUE')
      event.listen(tbl, 'after_create', spow.execute_if(dialect='somedb'))

      drop_spow = DDL('ALTER TABLE users SET secretpowers FALSE')
      connection.execute(drop_spow)

    When operating on Table events, the following ``statement``
    string substitions are available::

      %(table)s  - the Table name, with any required quoting applied
      %(schema)s - the schema name, with any required quoting applied
      %(fullname)s - the Table name including schema, quoted if needed

    The DDL's "context", if any, will be combined with the standard
    substutions noted above.  Keys present in the context will override
    the standard substitutions.

    """

    __visit_name__ = "ddl"

    def __init__(self, statement, on=None, context=None, bind=None):
        """Create a DDL statement.

        :param statement:
          A string or unicode string to be executed.  Statements will be
          processed with Python's string formatting operator.  See the
          ``context`` argument and the ``execute_at`` method.

          A literal '%' in a statement must be escaped as '%%'.

          SQL bind parameters are not available in DDL statements.

        :param on:
          .. deprecated:: 0.7
            See :meth:`.DDLElement.execute_if`.

          Optional filtering criteria.  May be a string, tuple or a callable
          predicate.  If a string, it will be compared to the name of the
          executing database dialect::

            DDL('something', on='postgresql')

          If a tuple, specifies multiple dialect names::

            DDL('something', on=('postgresql', 'mysql'))

          If a callable, it will be invoked with four positional arguments
          as well as optional keyword arguments:

            :ddl:
              This DDL element.

            :event:
              The name of the event that has triggered this DDL, such as
              'after-create' Will be None if the DDL is executed explicitly.

            :target:
              The ``Table`` or ``MetaData`` object which is the target of
              this event. May be None if the DDL is executed explicitly.

            :connection:
              The ``Connection`` being used for DDL execution

            :tables:
              Optional keyword argument - a list of Table objects which are to
              be created/ dropped within a MetaData.create_all() or drop_all()
              method call.


          If the callable returns a true value, the DDL statement will be
          executed.

        :param context:
          Optional dictionary, defaults to None.  These values will be
          available for use in string substitutions on the DDL statement.

        :param bind:
          Optional. A :class:`.Connectable`, used by
          default when ``execute()`` is invoked without a bind argument.


        .. seealso::

            :class:`.DDLEvents`

            :mod:`sqlalchemy.event`

        """

        if not isinstance(statement, util.string_types):
            raise exc.ArgumentError(
                "Expected a string or unicode SQL statement, got '%r'" %
                statement)

        self.statement = statement
        self.context = context or {}

        self._check_ddl_on(on)
        self.on = on
        self._bind = bind

    def __repr__(self):
        return '<%s@%s; %s>' % (
            type(self).__name__, id(self),
            ', '.join([repr(self.statement)] +
                      ['%s=%r' % (key, getattr(self, key))
                       for key in ('on', 'context')
                       if getattr(self, key)]))



class _CreateDropBase(DDLElement):
    """Base class for DDL constucts that represent CREATE and DROP or
    equivalents.

    The common theme of _CreateDropBase is a single
    ``element`` attribute which refers to the element
    to be created or dropped.

    """

    def __init__(self, element, on=None, bind=None):
        self.element = element
        self._check_ddl_on(on)
        self.on = on
        self.bind = bind

    def _create_rule_disable(self, compiler):
        """Allow disable of _create_rule using a callable.

        Pass to _create_rule using
        util.portable_instancemethod(self._create_rule_disable)
        to retain serializability.

        """
        return False


class CreateSchema(_CreateDropBase):
    """Represent a CREATE SCHEMA statement.

    .. versionadded:: 0.7.4

    The argument here is the string name of the schema.

    """

    __visit_name__ = "create_schema"

    def __init__(self, name, quote=None, **kw):
        """Create a new :class:`.CreateSchema` construct."""

        self.quote = quote
        super(CreateSchema, self).__init__(name, **kw)


class DropSchema(_CreateDropBase):
    """Represent a DROP SCHEMA statement.

    The argument here is the string name of the schema.

    .. versionadded:: 0.7.4

    """

    __visit_name__ = "drop_schema"

    def __init__(self, name, quote=None, cascade=False, **kw):
        """Create a new :class:`.DropSchema` construct."""

        self.quote = quote
        self.cascade = cascade
        super(DropSchema, self).__init__(name, **kw)


class CreateTable(_CreateDropBase):
    """Represent a CREATE TABLE statement."""

    __visit_name__ = "create_table"

    def __init__(self, element, on=None, bind=None):
        """Create a :class:`.CreateTable` construct.

        :param element: a :class:`.Table` that's the subject
         of the CREATE
        :param on: See the description for 'on' in :class:`.DDL`.
        :param bind: See the description for 'bind' in :class:`.DDL`.

        """
        super(CreateTable, self).__init__(element, on=on, bind=bind)
        self.columns = [CreateColumn(column)
            for column in element.columns
        ]


class _DropView(_CreateDropBase):
    """Semi-public 'DROP VIEW' construct.

    Used by the test suite for dialect-agnostic drops of views.
    This object will eventually be part of a public "view" API.

    """
    __visit_name__ = "drop_view"


class CreateColumn(_DDLCompiles):
    """Represent a :class:`.Column` as rendered in a CREATE TABLE statement,
    via the :class:`.CreateTable` construct.

    This is provided to support custom column DDL within the generation
    of CREATE TABLE statements, by using the
    compiler extension documented in :ref:`sqlalchemy.ext.compiler_toplevel`
    to extend :class:`.CreateColumn`.

    Typical integration is to examine the incoming :class:`.Column`
    object, and to redirect compilation if a particular flag or condition
    is found::

        from sqlalchemy import schema
        from sqlalchemy.ext.compiler import compiles

        @compiles(schema.CreateColumn)
        def compile(element, compiler, **kw):
            column = element.element

            if "special" not in column.info:
                return compiler.visit_create_column(element, **kw)

            text = "%s SPECIAL DIRECTIVE %s" % (
                    column.name,
                    compiler.type_compiler.process(column.type)
                )
            default = compiler.get_column_default_string(column)
            if default is not None:
                text += " DEFAULT " + default

            if not column.nullable:
                text += " NOT NULL"

            if column.constraints:
                text += " ".join(
                            compiler.process(const)
                            for const in column.constraints)
            return text

    The above construct can be applied to a :class:`.Table` as follows::

        from sqlalchemy import Table, Metadata, Column, Integer, String
        from sqlalchemy import schema

        metadata = MetaData()

        table = Table('mytable', MetaData(),
                Column('x', Integer, info={"special":True}, primary_key=True),
                Column('y', String(50)),
                Column('z', String(20), info={"special":True})
            )

        metadata.create_all(conn)

    Above, the directives we've added to the :attr:`.Column.info` collection
    will be detected by our custom compilation scheme::

        CREATE TABLE mytable (
                x SPECIAL DIRECTIVE INTEGER NOT NULL,
                y VARCHAR(50),
                z SPECIAL DIRECTIVE VARCHAR(20),
            PRIMARY KEY (x)
        )

    The :class:`.CreateColumn` construct can also be used to skip certain
    columns when producing a ``CREATE TABLE``.  This is accomplished by
    creating a compilation rule that conditionally returns ``None``.
    This is essentially how to produce the same effect as using the
    ``system=True`` argument on :class:`.Column`, which marks a column
    as an implicitly-present "system" column.

    For example, suppose we wish to produce a :class:`.Table` which skips
    rendering of the Postgresql ``xmin`` column against the Postgresql backend,
    but on other backends does render it, in anticipation of a triggered rule.
    A conditional compilation rule could skip this name only on Postgresql::

        from sqlalchemy.schema import CreateColumn

        @compiles(CreateColumn, "postgresql")
        def skip_xmin(element, compiler, **kw):
            if element.element.name == 'xmin':
                return None
            else:
                return compiler.visit_create_column(element, **kw)


        my_table = Table('mytable', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('xmin', Integer)
                )

    Above, a :class:`.CreateTable` construct will generate a ``CREATE TABLE``
    which only includes the ``id`` column in the string; the ``xmin`` column
    will be omitted, but only against the Postgresql backend.

    .. versionadded:: 0.8.3 The :class:`.CreateColumn` construct supports
       skipping of columns by returning ``None`` from a custom compilation rule.

    .. versionadded:: 0.8 The :class:`.CreateColumn` construct was added
       to support custom column creation styles.

    """
    __visit_name__ = 'create_column'

    def __init__(self, element):
        self.element = element


class DropTable(_CreateDropBase):
    """Represent a DROP TABLE statement."""

    __visit_name__ = "drop_table"


class CreateSequence(_CreateDropBase):
    """Represent a CREATE SEQUENCE statement."""

    __visit_name__ = "create_sequence"


class DropSequence(_CreateDropBase):
    """Represent a DROP SEQUENCE statement."""

    __visit_name__ = "drop_sequence"


class CreateIndex(_CreateDropBase):
    """Represent a CREATE INDEX statement."""

    __visit_name__ = "create_index"


class DropIndex(_CreateDropBase):
    """Represent a DROP INDEX statement."""

    __visit_name__ = "drop_index"


class AddConstraint(_CreateDropBase):
    """Represent an ALTER TABLE ADD CONSTRAINT statement."""

    __visit_name__ = "add_constraint"

    def __init__(self, element, *args, **kw):
        super(AddConstraint, self).__init__(element, *args, **kw)
        element._create_rule = util.portable_instancemethod(
                                            self._create_rule_disable)


class DropConstraint(_CreateDropBase):
    """Represent an ALTER TABLE DROP CONSTRAINT statement."""

    __visit_name__ = "drop_constraint"

    def __init__(self, element, cascade=False, **kw):
        self.cascade = cascade
        super(DropConstraint, self).__init__(element, **kw)
        element._create_rule = util.portable_instancemethod(
                                            self._create_rule_disable)


class DDLBase(SchemaVisitor):
    def __init__(self, connection):
        self.connection = connection


class SchemaGenerator(DDLBase):

    def __init__(self, dialect, connection, checkfirst=False,
                 tables=None, **kwargs):
        super(SchemaGenerator, self).__init__(connection, **kwargs)
        self.checkfirst = checkfirst
        self.tables = tables
        self.preparer = dialect.identifier_preparer
        self.dialect = dialect
        self.memo = {}

    def _can_create_table(self, table):
        self.dialect.validate_identifier(table.name)
        if table.schema:
            self.dialect.validate_identifier(table.schema)
        return not self.checkfirst or \
                not self.dialect.has_table(self.connection,
                                    table.name, schema=table.schema)

    def _can_create_sequence(self, sequence):
        return self.dialect.supports_sequences and \
            (
                (not self.dialect.sequences_optional or
                 not sequence.optional) and
                    (
                        not self.checkfirst or
                        not self.dialect.has_sequence(
                                self.connection,
                                sequence.name,
                                schema=sequence.schema)
                     )
            )

    def visit_metadata(self, metadata):
        if self.tables is not None:
            tables = self.tables
        else:
            tables = list(metadata.tables.values())
        collection = [t for t in sort_tables(tables)
                        if self._can_create_table(t)]
        seq_coll = [s for s in metadata._sequences.values()
                        if s.column is None and self._can_create_sequence(s)]

        metadata.dispatch.before_create(metadata, self.connection,
                                    tables=collection,
                                    checkfirst=self.checkfirst,
                                            _ddl_runner=self)

        for seq in seq_coll:
            self.traverse_single(seq, create_ok=True)

        for table in collection:
            self.traverse_single(table, create_ok=True)

        metadata.dispatch.after_create(metadata, self.connection,
                                    tables=collection,
                                    checkfirst=self.checkfirst,
                                            _ddl_runner=self)

    def visit_table(self, table, create_ok=False):
        if not create_ok and not self._can_create_table(table):
            return

        table.dispatch.before_create(table, self.connection,
                                        checkfirst=self.checkfirst,
                                            _ddl_runner=self)

        for column in table.columns:
            if column.default is not None:
                self.traverse_single(column.default)

        self.connection.execute(CreateTable(table))

        if hasattr(table, 'indexes'):
            for index in table.indexes:
                self.traverse_single(index)

        table.dispatch.after_create(table, self.connection,
                                        checkfirst=self.checkfirst,
                                            _ddl_runner=self)

    def visit_sequence(self, sequence, create_ok=False):
        if not create_ok and not self._can_create_sequence(sequence):
            return
        self.connection.execute(CreateSequence(sequence))

    def visit_index(self, index):
        self.connection.execute(CreateIndex(index))


class SchemaDropper(DDLBase):

    def __init__(self, dialect, connection, checkfirst=False,
                 tables=None, **kwargs):
        super(SchemaDropper, self).__init__(connection, **kwargs)
        self.checkfirst = checkfirst
        self.tables = tables
        self.preparer = dialect.identifier_preparer
        self.dialect = dialect
        self.memo = {}

    def visit_metadata(self, metadata):
        if self.tables is not None:
            tables = self.tables
        else:
            tables = list(metadata.tables.values())

        collection = [
            t
            for t in reversed(sort_tables(tables))
            if self._can_drop_table(t)
        ]

        seq_coll = [
            s
            for s in metadata._sequences.values()
            if s.column is None and self._can_drop_sequence(s)
        ]

        metadata.dispatch.before_drop(
            metadata, self.connection, tables=collection,
            checkfirst=self.checkfirst, _ddl_runner=self)

        for table in collection:
            self.traverse_single(table, drop_ok=True)

        for seq in seq_coll:
            self.traverse_single(seq, drop_ok=True)

        metadata.dispatch.after_drop(
            metadata, self.connection, tables=collection,
            checkfirst=self.checkfirst, _ddl_runner=self)

    def _can_drop_table(self, table):
        self.dialect.validate_identifier(table.name)
        if table.schema:
            self.dialect.validate_identifier(table.schema)
        return not self.checkfirst or self.dialect.has_table(self.connection,
                                            table.name, schema=table.schema)

    def _can_drop_sequence(self, sequence):
        return self.dialect.supports_sequences and \
            ((not self.dialect.sequences_optional or
                 not sequence.optional) and
                (not self.checkfirst or
                self.dialect.has_sequence(
                                self.connection,
                                sequence.name,
                                schema=sequence.schema))
            )

    def visit_index(self, index):
        self.connection.execute(DropIndex(index))

    def visit_table(self, table, drop_ok=False):
        if not drop_ok and not self._can_drop_table(table):
            return

        table.dispatch.before_drop(table, self.connection,
                                    checkfirst=self.checkfirst,
                                            _ddl_runner=self)

        for column in table.columns:
            if column.default is not None:
                self.traverse_single(column.default)

        self.connection.execute(DropTable(table))

        table.dispatch.after_drop(table, self.connection,
                                        checkfirst=self.checkfirst,
                                            _ddl_runner=self)

    def visit_sequence(self, sequence, drop_ok=False):
        if not drop_ok and not self._can_drop_sequence(sequence):
            return
        self.connection.execute(DropSequence(sequence))

def sort_tables(tables, skip_fn=None, extra_dependencies=None):
    """sort a collection of Table objects in order of
                their foreign-key dependency."""

    tables = list(tables)
    tuples = []
    if extra_dependencies is not None:
        tuples.extend(extra_dependencies)

    def visit_foreign_key(fkey):
        if fkey.use_alter:
            return
        elif skip_fn and skip_fn(fkey):
            return
        parent_table = fkey.column.table
        if parent_table in tables:
            child_table = fkey.parent.table
            if parent_table is not child_table:
                tuples.append((parent_table, child_table))

    for table in tables:
        traverse(table,
                            {'schema_visitor': True},
                            {'foreign_key': visit_foreign_key})

        tuples.extend(
            [parent, table] for parent in table._extra_dependencies
        )

    return list(topological.sort(tuples, tables))

