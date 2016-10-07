:orphan:

.. _glossary:

========
Glossary
========

.. glossary::
    :sorted:

    annotations
        Annotations are a concept used internally by SQLAlchemy in order to store
        additional information along with :class:`.ClauseElement` objects.  A Python
        dictionary is associated with a copy of the object, which contains key/value
        pairs significant to various internal systems, mostly within the ORM::

            some_column = Column('some_column', Integer)
            some_column_annotated = some_column._annotate({"entity": User})

        The annotation system differs from the public dictionary :attr:`.Column.info`
        in that the above annotation operation creates a *copy* of the new :class:`.Column`,
        rather than considering all annotation values to be part of a single
        unit.  The ORM creates copies of expression objects in order to
        apply annotations that are specific to their context, such as to differentiate
        columns that should render themselves as relative to a joined-inheritance
        entity versus those which should render relative to their immediate parent
        table alone, as well as to differentiate columns within the "join condition"
        of a relationship where the column in some cases needs to be expressed
        in terms of one particular table alias or another, based on its position
        within the join expression.

    crud
        An acronym meaning "Create, Update, Delete".  The term in SQL refers to the
        set of operations that create, modify and delete data from the database,
        also known as :term:`DML`, and typically refers to the ``INSERT``,
        ``UPDATE``, and ``DELETE`` statements.

    descriptor
    descriptors
        In Python, a descriptor is an object attribute with “binding behavior”, one whose attribute access has been overridden by methods in the `descriptor protocol <http://docs.python.org/howto/descriptor.html>`_.
        Those methods are __get__(), __set__(), and __delete__(). If any of those methods are defined
        for an object, it is said to be a descriptor.

        In SQLAlchemy, descriptors are used heavily in order to provide attribute behavior
        on mapped classes.   When a class is mapped as such::

            class MyClass(Base):
                __tablename__ = 'foo'

                id = Column(Integer, primary_key=True)
                data = Column(String)

        The ``MyClass`` class will be :term:`mapped` when its definition
        is complete, at which point the ``id`` and ``data`` attributes,
        starting out as :class:`.Column` objects, will be replaced
        by the :term:`instrumentation` system with instances
        of :class:`.InstrumentedAttribute`, which are descriptors that
        provide the above mentioned ``__get__()``, ``__set__()`` and
        ``__delete__()`` methods.   The :class:`.InstrumentedAttribute`
        will generate a SQL expression when used at the class level::

            >>> print(MyClass.data == 5)
            data = :data_1

        and at the instance level, keeps track of changes to values,
        and also :term:`lazy loads` unloaded attributes
        from the database::

            >>> m1 = MyClass()
            >>> m1.id = 5
            >>> m1.data = "some data"

            >>> from sqlalchemy import inspect
            >>> inspect(m1).attrs.data.history.added
            "some data"

    DDL
        An acronym for *Data Definition Language*.  DDL is the subset
        of SQL that relational databases use to configure tables, constraints,
        and other permanent objects within a database schema.  SQLAlchemy
        provides a rich API for constructing and emitting DDL expressions.

        .. seealso::

            :ref:`metadata_toplevel`

            `DDL (via Wikipedia) <http://en.wikipedia.org/wiki/Data_definition_language>`_

    discriminator
        A result-set column which is used during :term:`polymorphic` loading
        to determine what kind of mapped class should be applied to a particular
        incoming result row.   In SQLAlchemy, the classes are always part
        of a hierarchy mapping using inheritance mapping.

        .. seealso::

            :ref:`inheritance_toplevel`

    instrumentation
    instrumented
    instrumenting
        Instrumentation refers to the process of augmenting the functionality
        and attribute set of a particular class.   Ideally, the
        behavior of the class should remain close to a regular
        class, except that additional behaviors and features are
        made available.  The SQLAlchemy :term:`mapping` process,
        among other things, adds database-enabled :term:`descriptors`
        to a mapped
        class which each represent a particular database column
        or relationship to a related class.

    identity map
        A mapping between Python objects and their database identities.
        The identity map is a collection that's associated with an
        ORM :term:`session` object, and maintains a single instance
        of every database object keyed to its identity.   The advantage
        to this pattern is that all operations which occur for a particular
        database identity are transparently coordinated onto a single
        object instance.  When using an identity map in conjunction with
        an :term:`isolated` transaction, having a reference
        to an object that's known to have a particular primary key can
        be considered from a practical standpoint to be a
        proxy to the actual database row.

        .. seealso::

            Martin Fowler - Identity Map - http://martinfowler.com/eaaCatalog/identityMap.html

    lazy load
    lazy loads
    lazy loaded
    lazy loading
        In object relational mapping, a "lazy load" refers to an
        attribute that does not contain its database-side value
        for some period of time, typically when the object is
        first loaded.  Instead, the attribute receives a
        *memoization* that causes it to go out to the database
        and load its data when it's first used.   Using this pattern,
        the complexity and time spent within object fetches can
        sometimes be reduced, in that
        attributes for related tables don't need to be addressed
        immediately.

        .. seealso::

            `Lazy Load (on Martin Fowler) <http://martinfowler.com/eaaCatalog/lazyLoad.html>`_

            :term:`N plus one problem`

            :doc:`orm/loading_relationships`

    mapping
    mapped
        We say a class is "mapped" when it has been passed through the
        :func:`.orm.mapper` function.   This process associates the
        class with a database table or other :term:`selectable`
        construct, so that instances of it can be persisted
        using a :class:`.Session` as well as loaded using a
        :class:`.Query`.

    N plus one problem
        The N plus one problem is a common side effect of the
        :term:`lazy load` pattern, whereby an application wishes
        to iterate through a related attribute or collection on
        each member of a result set of objects, where that
        attribute or collection is set to be loaded via the lazy
        load pattern.   The net result is that a SELECT statement
        is emitted to load the initial result set of parent objects;
        then, as the application iterates through each member,
        an additional SELECT statement is emitted for each member
        in order to load the related attribute or collection for
        that member.  The end result is that for a result set of
        N parent objects, there will be N + 1 SELECT statements emitted.

        The N plus one problem is alleviated using :term:`eager loading`.

        .. seealso::

            :doc:`orm/loading_relationships`

    polymorphic
    polymorphically
        Refers to a function that handles several types at once.  In SQLAlchemy,
        the term is usually applied to the concept of an ORM mapped class
        whereby a query operation will return different subclasses
        based on information in the result set, typically by checking the
        value of a particular column in the result known as the :term:`discriminator`.

        Polymorphic loading in SQLAlchemy implies that a one or a
        combination of three different schemes are used to map a hierarchy
        of classes; "joined", "single", and "concrete".   The section
        :ref:`inheritance_toplevel` describes inheritance mapping fully.

    generative
        A term that SQLAlchemy uses to refer what's normally known
        as :term:`method chaining`; see that term for details.

    method chaining
        An object-oriented technique whereby the state of an object
        is constructed by calling methods on the object.   The
        object features any number of methods, each of which return
        a new object (or in some cases the same object) with
        additional state added to the object.

        The two SQLAlchemy objects that make the most use of
        method chaining are the :class:`~.expression.Select`
        object and the :class:`~.orm.query.Query` object.
        For example, a :class:`~.expression.Select` object can
        be assigned two expressions to its WHERE clause as well
        as an ORDER BY clause by calling upon the :meth:`~.Select.where`
        and :meth:`~.Select.order_by` methods::

            stmt = select([user.c.name]).\
                        where(user.c.id > 5).\
                        where(user.c.name.like('e%').\
                        order_by(user.c.name)

        Each method call above returns a copy of the original
        :class:`~.expression.Select` object with additional qualifiers
        added.

        .. seealso::

            :term:`generative`

    release
    releases
    released
        In the context of SQLAlchemy, the term "released"
        refers to the process of ending the usage of a particular
        database connection.    SQLAlchemy features the usage
        of connection pools, which allows configurability as to
        the lifespan of database connections.   When using a pooled
        connection, the process of "closing" it, i.e. invoking
        a statement like ``connection.close()``, may have the effect
        of the connection being returned to an existing pool,
        or it may have the effect of actually shutting down the
        underlying TCP/IP connection referred to by that connection -
        which one takes place depends on configuration as well
        as the current state of the pool.  So we used the term
        *released* instead, to mean "do whatever it is you do
        with connections when we're done using them".

        The term will sometimes be used in the phrase, "release
        transactional resources", to indicate more explicitly that
        what we are actually "releasing" is any transactional
        state which as accumulated upon the connection.  In most
        situations, the process of selecting from tables, emitting
        updates, etc. acquires :term:`isolated` state upon
        that connection as well as potential row or table locks.
        This state is all local to a particular transaction
        on the connection, and is released when we emit a rollback.
        An important feature of the connection pool is that when
        we return a connection to the pool, the ``connection.rollback()``
        method of the DBAPI is called as well, so that as the
        connection is set up to be used again, it's in a "clean"
        state with no references held to the previous series
        of operations.

        .. seealso::

        	:ref:`pooling_toplevel`

    DBAPI
        DBAPI is shorthand for the phrase "Python Database API
        Specification".  This is a widely used specification
        within Python to define common usage patterns for all
        database connection packages.   The DBAPI is a "low level"
        API which is typically the lowest level system used
        in a Python application to talk to a database.  SQLAlchemy's
        :term:`dialect` system is constructed around the
        operation of the DBAPI, providing individual dialect
        classes which service a specific DBAPI on top of a
        specific database engine; for example, the :func:`.create_engine`
        URL ``postgresql+psycopg2://@localhost/test``
        refers to the :mod:`psycopg2 <.postgresql.psycopg2>`
        DBAPI/dialect combination, whereas the URL ``mysql+mysqldb://@localhost/test``
        refers to the :mod:`MySQL for Python <.mysql.mysqldb>`
        DBAPI DBAPI/dialect combination.

        .. seealso::

            `PEP 249 - Python Database API Specification v2.0 <http://www.python.org/dev/peps/pep-0249/>`_

    domain model

        A domain model in problem solving and software engineering is a conceptual model of all the topics related to a specific problem. It describes the various entities, their attributes, roles, and relationships, plus the constraints that govern the problem domain.

        (via Wikipedia)

        .. seealso::

            `Domain Model (wikipedia) <http://en.wikipedia.org/wiki/Domain_model>`_

    unit of work
        This pattern is where the system transparently keeps
        track of changes to objects and periodically flushes all those
        pending changes out to the database. SQLAlchemy's Session
        implements this pattern fully in a manner similar to that of
        Hibernate.

        .. seealso::

            `Unit of Work by Martin Fowler <http://martinfowler.com/eaaCatalog/unitOfWork.html>`_

            :doc:`orm/session`

    expire
    expires
    expiring
        In the SQLAlchemy ORM, refers to when the data in a :term:`persistent`
        or sometimes :term:`detached` object is erased, such that when
        the object's attributes are next accessed, a :term:`lazy load` SQL
        query will be emitted in order to refresh the data for this object
        as stored in the current ongoing transaction.

        .. seealso::

            :ref:`session_expire`

    Session
        The container or scope for ORM database operations. Sessions
        load instances from the database, track changes to mapped
        instances and persist changes in a single unit of work when
        flushed.

        .. seealso::

            :doc:`orm/session`

    columns clause
        The portion of the ``SELECT`` statement which enumerates the
        SQL expressions to be returned in the result set.  The expressions
        follow the ``SELECT`` keyword directly and are a comma-separated
        list of individual expressions.

        E.g.:

        .. sourcecode:: sql

            SELECT user_account.name, user_account.email
            FROM user_account WHERE user_account.name = 'fred'

        Above, the list of columns ``user_acount.name``,
        ``user_account.email`` is the columns clause of the ``SELECT``.

    WHERE clause
        The portion of the ``SELECT`` statement which indicates criteria
        by which rows should be filtered.   It is a single SQL expression
        which follows the keyword ``WHERE``.

        .. sourcecode:: sql

            SELECT user_account.name, user_account.email
            FROM user_account
            WHERE user_account.name = 'fred' AND user_account.status = 'E'

        Above, the phrase ``WHERE user_account.name = 'fred' AND user_account.status = 'E'``
        comprises the WHERE clause of the ``SELECT``.

    FROM clause
        The portion of the ``SELECT`` statement which indicates the initial
        source of rows.

        A simple ``SELECT`` will feature one or more table names in its
        FROM clause.  Multiple sources are separated by a comma:

        .. sourcecode:: sql

            SELECT user.name, address.email_address
            FROM user, address
            WHERE user.id=address.user_id

        The FROM clause is also where explicit joins are specified.  We can
        rewrite the above ``SELECT`` using a single ``FROM`` element which consists
        of a ``JOIN`` of the two tables:

        .. sourcecode:: sql

            SELECT user.name, address.email_address
            FROM user JOIN address ON user.id=address.user_id


    subquery
        Refers to a ``SELECT`` statement that is embedded within an enclosing
        ``SELECT``.

        A subquery comes in two general flavors, one known as a "scalar select"
        which specifically must return exactly one row and one column, and the
        other form which acts as a "derived table" and serves as a source of
        rows for the FROM clause of another select.  A scalar select is eligible
        to be placed in the :term:`WHERE clause`, :term:`columns clause`,
        ORDER BY clause or HAVING clause of the enclosing select, whereas the
        derived table form is eligible to be placed in the FROM clause of the
        enclosing ``SELECT``.

        Examples:

        1. a scalar subquery placed in the :term:`columns clause` of an enclosing
           ``SELECT``.  The subquery in this example is a :term:`correlated subquery` because part
           of the rows which it selects from are given via the enclosing statement.

           .. sourcecode:: sql

            SELECT id, (SELECT name FROM address WHERE address.user_id=user.id)
            FROM user

        2. a scalar subquery placed in the :term:`WHERE clause` of an enclosing
           ``SELECT``.  This subquery in this example is not correlated as it selects a fixed result.

           .. sourcecode:: sql

            SELECT id, name FROM user
            WHERE status=(SELECT status_id FROM status_code WHERE code='C')

        3. a derived table subquery placed in the :term:`FROM clause` of an enclosing
           ``SELECT``.   Such a subquery is almost always given an alias name.

           .. sourcecode:: sql

            SELECT user.id, user.name, ad_subq.email_address
            FROM
                user JOIN
                (select user_id, email_address FROM address WHERE address_type='Q') AS ad_subq
                ON user.id = ad_subq.user_id

    correlates
    correlated subquery
    correlated subqueries
        A :term:`subquery` is correlated if it depends on data in the
        enclosing ``SELECT``.

        Below, a subquery selects the aggregate value ``MIN(a.id)``
        from the ``email_address`` table, such that
        it will be invoked for each value of ``user_account.id``, correlating
        the value of this column against the ``email_address.user_account_id``
        column:

        .. sourcecode:: sql

            SELECT user_account.name, email_address.email
             FROM user_account
             JOIN email_address ON user_account.id=email_address.user_account_id
             WHERE email_address.id = (
                SELECT MIN(a.id) FROM email_address AS a
                WHERE a.user_account_id=user_account.id
             )

        The above subquery refers to the ``user_account`` table, which is not itself
        in the ``FROM`` clause of this nested query.   Instead, the ``user_account``
        table is received from the enclosing query, where each row selected from
        ``user_account`` results in a distinct execution of the subquery.

        A correlated subquery is in most cases present in the :term:`WHERE clause`
        or :term:`columns clause` of the immediately enclosing ``SELECT``
        statement, as well as in the ORDER BY or HAVING clause.

        In less common cases, a correlated subquery may be present in the
        :term:`FROM clause` of an enclosing ``SELECT``; in these cases the
        correlation is typically due to the enclosing ``SELECT`` itself being
        enclosed in the WHERE,
        ORDER BY, columns or HAVING clause of another ``SELECT``, such as:

        .. sourcecode:: sql

            SELECT parent.id FROM parent
            WHERE EXISTS (
                SELECT * FROM (
                    SELECT child.id AS id, child.parent_id AS parent_id, child.pos AS pos
                    FROM child
                    WHERE child.parent_id = parent.id ORDER BY child.pos
                LIMIT 3)
            WHERE id = 7)

        Correlation from one ``SELECT`` directly to one which encloses the correlated
        query via its ``FROM``
        clause is not possible, because the correlation can only proceed once the
        original source rows from the enclosing statement's FROM clause are available.


    ACID
    ACID model
        An acronym for "Atomicity, Consistency, Isolation,
        Durability"; a set of properties that guarantee that
        database transactions are processed reliably.
        (via Wikipedia)

        .. seealso::

            :term:`atomicity`

            :term:`consistency`

            :term:`isolation`

            :term:`durability`

            http://en.wikipedia.org/wiki/ACID_Model

    atomicity
        Atomicity is one of the components of the :term:`ACID` model,
        and requires that each transaction is "all or nothing":
        if one part of the transaction fails, the entire transaction
        fails, and the database state is left unchanged. An atomic
        system must guarantee atomicity in each and every situation,
        including power failures, errors, and crashes.
        (via Wikipedia)

        .. seealso::

            :term:`ACID`

            http://en.wikipedia.org/wiki/Atomicity_(database_systems)

    consistency
        Consistency is one of the compoments of the :term:`ACID` model,
        and ensures that any transaction will
        bring the database from one valid state to another. Any data
        written to the database must be valid according to all defined
        rules, including but not limited to :term:`constraints`, cascades,
        triggers, and any combination thereof.
        (via Wikipedia)

        .. seealso::

            :term:`ACID`

            http://en.wikipedia.org/wiki/Consistency_(database_systems)

    isolation
    isolated
        The isolation property of the :term:`ACID` model
        ensures that the concurrent execution
        of transactions results in a system state that would be
        obtained if transactions were executed serially, i.e. one
        after the other. Each transaction must execute in total
        isolation i.e. if T1 and T2 execute concurrently then each
        should remain independent of the other.
        (via Wikipedia)

        .. seealso::

            :term:`ACID`

            http://en.wikipedia.org/wiki/Isolation_(database_systems)

    durability
        Durability is a property of the :term:`ACID` model
        which means that once a transaction has been committed,
        it will remain so, even in the event of power loss, crashes,
        or errors. In a relational database, for instance, once a
        group of SQL statements execute, the results need to be stored
        permanently (even if the database crashes immediately
        thereafter).
        (via Wikipedia)

        .. seealso::

            :term:`ACID`

            http://en.wikipedia.org/wiki/Durability_(database_systems)

    RETURNING
        This is a non-SQL standard clause provided in various forms by
        certain backends, which provides the service of returning a result
        set upon execution of an INSERT, UPDATE or DELETE statement.  Any set
        of columns from the matched rows can be returned, as though they were
        produced from a SELECT statement.

        The RETURNING clause provides both a dramatic performance boost to
        common update/select scenarios, including retrieval of inline- or
        default- generated primary key values and defaults at the moment they
        were created, as well as a way to get at server-generated
        default values in an atomic way.

        An example of RETURNING, idiomatic to PostgreSQL, looks like::

            INSERT INTO user_account (name) VALUES ('new name') RETURNING id, timestamp

        Above, the INSERT statement will provide upon execution a result set
        which includes the values of the columns ``user_account.id`` and
        ``user_account.timestamp``, which above should have been generated as default
        values as they are not included otherwise (but note any series of columns
        or SQL expressions can be placed into RETURNING, not just default-value columns).

        The backends that currently support
        RETURNING or a similar construct are PostgreSQL, SQL Server, Oracle,
        and Firebird.    The PostgreSQL and Firebird implementations are generally
        full featured, whereas the implementations of SQL Server and Oracle
        have caveats. On SQL Server, the clause is known as "OUTPUT INSERTED"
        for INSERT and UPDATE statements and "OUTPUT DELETED" for DELETE statements;
        the key caveat is that triggers are not supported in conjunction with this
        keyword.  On Oracle, it is known as "RETURNING...INTO", and requires that the
        value be placed into an OUT parameter, meaning not only is the syntax awkward,
        but it can also only be used for one row at a time.

        SQLAlchemy's :meth:`.UpdateBase.returning` system provides a layer of abstraction
        on top of the RETURNING systems of these backends to provide a consistent
        interface for returning columns.  The ORM also includes many optimizations
        that make use of RETURNING when available.

    one to many
        A style of :func:`~sqlalchemy.orm.relationship` which links
        the primary key of the parent mapper's table to the foreign
        key of a related table.   Each unique parent object can
        then refer to zero or more unique related objects.

        The related objects in turn will have an implicit or
        explicit :term:`many to one` relationship to their parent
        object.

        An example one to many schema (which, note, is identical
        to the :term:`many to one` schema):

        .. sourcecode:: sql

            CREATE TABLE department (
                id INTEGER PRIMARY KEY,
                name VARCHAR(30)
            )

            CREATE TABLE employee (
                id INTEGER PRIMARY KEY,
                name VARCHAR(30),
                dep_id INTEGER REFERENCES department(id)
            )

        The relationship from ``department`` to ``employee`` is
        one to many, since many employee records can be associated with a
        single department.  A SQLAlchemy mapping might look like::

            class Department(Base):
                __tablename__ = 'department'
                id = Column(Integer, primary_key=True)
                name = Column(String(30))
                employees = relationship("Employee")

            class Employee(Base):
                __tablename__ = 'employee'
                id = Column(Integer, primary_key=True)
                name = Column(String(30))
                dep_id = Column(Integer, ForeignKey('department.id'))

        .. seealso::

            :term:`relationship`

            :term:`many to one`

            :term:`backref`

    many to one
        A style of :func:`~sqlalchemy.orm.relationship` which links
        a foreign key in the parent mapper's table to the primary
        key of a related table.   Each parent object can
        then refer to exactly zero or one related object.

        The related objects in turn will have an implicit or
        explicit :term:`one to many` relationship to any number
        of parent objects that refer to them.

        An example many to one schema (which, note, is identical
        to the :term:`one to many` schema):

        .. sourcecode:: sql

            CREATE TABLE department (
                id INTEGER PRIMARY KEY,
                name VARCHAR(30)
            )

            CREATE TABLE employee (
                id INTEGER PRIMARY KEY,
                name VARCHAR(30),
                dep_id INTEGER REFERENCES department(id)
            )


        The relationship from ``employee`` to ``department`` is
        many to one, since many employee records can be associated with a
        single department.  A SQLAlchemy mapping might look like::

            class Department(Base):
                __tablename__ = 'department'
                id = Column(Integer, primary_key=True)
                name = Column(String(30))

            class Employee(Base):
                __tablename__ = 'employee'
                id = Column(Integer, primary_key=True)
                name = Column(String(30))
                dep_id = Column(Integer, ForeignKey('department.id'))
                department = relationship("Department")

        .. seealso::

            :term:`relationship`

            :term:`one to many`

            :term:`backref`

    backref
    bidirectional relationship
        An extension to the :term:`relationship` system whereby two
        distinct :func:`~sqlalchemy.orm.relationship` objects can be
        mutually associated with each other, such that they coordinate
        in memory as changes occur to either side.   The most common
        way these two relationships are constructed is by using
        the :func:`~sqlalchemy.orm.relationship` function explicitly
        for one side and specifying the ``backref`` keyword to it so that
        the other :func:`~sqlalchemy.orm.relationship` is created
        automatically.  We can illustrate this against the example we've
        used in :term:`one to many` as follows::

            class Department(Base):
                __tablename__ = 'department'
                id = Column(Integer, primary_key=True)
                name = Column(String(30))
                employees = relationship("Employee", backref="department")

            class Employee(Base):
                __tablename__ = 'employee'
                id = Column(Integer, primary_key=True)
                name = Column(String(30))
                dep_id = Column(Integer, ForeignKey('department.id'))

        A backref can be applied to any relationship, including one to many,
        many to one, and :term:`many to many`.

        .. seealso::

            :term:`relationship`

            :term:`one to many`

            :term:`many to one`

            :term:`many to many`

    many to many
        A style of :func:`sqlalchemy.orm.relationship` which links two tables together
        via an intermediary table in the middle.   Using this configuration,
        any number of rows on the left side may refer to any number of
        rows on the right, and vice versa.

        A schema where employees can be associated with projects:

        .. sourcecode:: sql

            CREATE TABLE employee (
                id INTEGER PRIMARY KEY,
                name VARCHAR(30)
            )

            CREATE TABLE project (
                id INTEGER PRIMARY KEY,
                name VARCHAR(30)
            )

            CREATE TABLE employee_project (
                employee_id INTEGER PRIMARY KEY,
                project_id INTEGER PRIMARY KEY,
                FOREIGN KEY employee_id REFERENCES employee(id),
                FOREIGN KEY project_id REFERENCES project(id)
            )

        Above, the ``employee_project`` table is the many-to-many table,
        which naturally forms a composite primary key consisting
        of the primary key from each related table.

        In SQLAlchemy, the :func:`sqlalchemy.orm.relationship` function
        can represent this style of relationship in a mostly
        transparent fashion, where the many-to-many table is
        specified using plain table metadata::

            class Employee(Base):
                __tablename__ = 'employee'

                id = Column(Integer, primary_key)
                name = Column(String(30))

                projects = relationship(
                    "Project",
                    secondary=Table('employee_project', Base.metadata,
                                Column("employee_id", Integer, ForeignKey('employee.id'),
                                            primary_key=True),
                                Column("project_id", Integer, ForeignKey('project.id'),
                                            primary_key=True)
                            ),
                    backref="employees"
                    )

            class Project(Base):
                __tablename__ = 'project'

                id = Column(Integer, primary_key)
                name = Column(String(30))

        Above, the ``Employee.projects`` and back-referencing ``Project.employees``
        collections are defined::

            proj = Project(name="Client A")

            emp1 = Employee(name="emp1")
            emp2 = Employee(name="emp2")

            proj.employees.extend([emp1, emp2])

        .. seealso::

            :term:`association relationship`

            :term:`relationship`

            :term:`one to many`

            :term:`many to one`

    relationship
    relationships
        A connecting unit between two mapped classes, corresponding
        to some relationship between the two tables in the database.

        The relationship is defined using the SQLAlchemy function
        :func:`~sqlalchemy.orm.relationship`.   Once created, SQLAlchemy
        inspects the arguments and underlying mappings involved
        in order to classify the relationship as one of three types:
        :term:`one to many`, :term:`many to one`, or :term:`many to many`.
        With this classification, the relationship construct
        handles the task of persisting the appropriate linkages
        in the database in response to in-memory object associations,
        as well as the job of loading object references and collections
        into memory based on the current linkages in the
        database.

        .. seealso::

            :ref:`relationship_config_toplevel`

    association relationship
        A two-tiered :term:`relationship` which links two tables
        together using an association table in the middle.  The
        association relationship differs from a :term:`many to many`
        relationship in that the many-to-many table is mapped
        by a full class, rather than invisibly handled by the
        :func:`sqlalchemy.orm.relationship` construct as in the case
        with many-to-many, so that additional attributes are
        explicitly available.

        For example, if we wanted to associate employees with
        projects, also storing the specific role for that employee
        with the project, the relational schema might look like:

        .. sourcecode:: sql

            CREATE TABLE employee (
                id INTEGER PRIMARY KEY,
                name VARCHAR(30)
            )

            CREATE TABLE project (
                id INTEGER PRIMARY KEY,
                name VARCHAR(30)
            )

            CREATE TABLE employee_project (
                employee_id INTEGER PRIMARY KEY,
                project_id INTEGER PRIMARY KEY,
                role_name VARCHAR(30),
                FOREIGN KEY employee_id REFERENCES employee(id),
                FOREIGN KEY project_id REFERENCES project(id)
            )

        A SQLAlchemy declarative mapping for the above might look like::

            class Employee(Base):
                __tablename__ = 'employee'

                id = Column(Integer, primary_key)
                name = Column(String(30))


            class Project(Base):
                __tablename__ = 'project'

                id = Column(Integer, primary_key)
                name = Column(String(30))


            class EmployeeProject(Base):
                __tablename__ = 'employee_project'

                employee_id = Column(Integer, ForeignKey('employee.id'), primary_key=True)
                project_id = Column(Integer, ForeignKey('project.id'), primary_key=True)
                role_name = Column(String(30))

                project = relationship("Project", backref="project_employees")
                employee = relationship("Employee", backref="employee_projects")


        Employees can be added to a project given a role name::

            proj = Project(name="Client A")

            emp1 = Employee(name="emp1")
            emp2 = Employee(name="emp2")

            proj.project_employees.extend([
                EmployeeProject(employee=emp1, role="tech lead"),
                EmployeeProject(employee=emp2, role="account executive")
            ])

        .. seealso::

            :term:`many to many`

    constraint
    constraints
    constrained
        Rules established within a relational database that ensure
        the validity and consistency of data.   Common forms
        of constraint include :term:`primary key constraint`,
        :term:`foreign key constraint`, and :term:`check constraint`.

    candidate key

        A :term:`relational algebra` term referring to an attribute or set
        of attributes that form a uniquely identifying key for a
        row.  A row may have more than one candidate key, each of which
        is suitable for use as the primary key of that row.
        The primary key of a table is always a candidate key.

        .. seealso::

            :term:`primary key`

            http://en.wikipedia.org/wiki/Candidate_key

    primary key
    primary key constraint

        A :term:`constraint` that uniquely defines the characteristics
        of each :term:`row`. The primary key has to consist of
        characteristics that cannot be duplicated by any other row.
        The primary key may consist of a single attribute or
        multiple attributes in combination.
        (via Wikipedia)

        The primary key of a table is typically, though not always,
        defined within the ``CREATE TABLE`` :term:`DDL`:

        .. sourcecode:: sql

            CREATE TABLE employee (
                 emp_id INTEGER,
                 emp_name VARCHAR(30),
                 dep_id INTEGER,
                 PRIMARY KEY (emp_id)
            )

        .. seealso::

            http://en.wikipedia.org/wiki/Primary_Key

    foreign key constraint
        A referential constraint between two tables.  A foreign key is a field or set of fields in a
        relational table that matches a :term:`candidate key` of another table.
        The foreign key can be used to cross-reference tables.
        (via Wikipedia)

        A foreign key constraint can be added to a table in standard
        SQL using :term:`DDL` like the following:

        .. sourcecode:: sql

            ALTER TABLE employee ADD CONSTRAINT dep_id_fk
            FOREIGN KEY (employee) REFERENCES department (dep_id)

        .. seealso::

            http://en.wikipedia.org/wiki/Foreign_key_constraint

    check constraint

        A check constraint is a
        condition that defines valid data when adding or updating an
        entry in a table of a relational database. A check constraint
        is applied to each row in the table.

        (via Wikipedia)

        A check constraint can be added to a table in standard
        SQL using :term:`DDL` like the following:

        .. sourcecode:: sql

            ALTER TABLE distributors ADD CONSTRAINT zipchk CHECK (char_length(zipcode) = 5);

        .. seealso::

            http://en.wikipedia.org/wiki/Check_constraint

    unique constraint
    unique key index
        A unique key index can uniquely identify each row of data
        values in a database table. A unique key index comprises a
        single column or a set of columns in a single database table.
        No two distinct rows or data records in a database table can
        have the same data value (or combination of data values) in
        those unique key index columns if NULL values are not used.
        Depending on its design, a database table may have many unique
        key indexes but at most one primary key index.

        (via Wikipedia)

        .. seealso::

            http://en.wikipedia.org/wiki/Unique_key#Defining_unique_keys

    transient
        This describes one of the major object states which
        an object can have within a :term:`session`; a transient object
        is a new object that doesn't have any database identity
        and has not been associated with a session yet.  When the
        object is added to the session, it moves to the
        :term:`pending` state.

        .. seealso::

            :ref:`session_object_states`

    pending
        This describes one of the major object states which
        an object can have within a :term:`session`; a pending object
        is a new object that doesn't have any database identity,
        but has been recently associated with a session.   When
        the session emits a flush and the row is inserted, the
        object moves to the :term:`persistent` state.

        .. seealso::

            :ref:`session_object_states`

    deleted
        This describes one of the major object states which
        an object can have within a :term:`session`; a deleted object
        is an object that was formerly persistent and has had a
        DELETE statement emitted to the database within a flush
        to delete its row.  The object will move to the :term:`detached`
        state once the session's transaction is committed; alternatively,
        if the session's transaction is rolled back, the DELETE is
        reverted and the object moves back to the :term:`persistent`
        state.

        .. seealso::

            :ref:`session_object_states`

    persistent
        This describes one of the major object states which
        an object can have within a :term:`session`; a persistent object
        is an object that has a database identity (i.e. a primary key)
        and is currently associated with a session.   Any object
        that was previously :term:`pending` and has now been inserted
        is in the persistent state, as is any object that's
        been loaded by the session from the database.   When a
        persistent object is removed from a session, it is known
        as :term:`detached`.

        .. seealso::

            :ref:`session_object_states`

    detached
        This describes one of the major object states which
        an object can have within a :term:`session`; a detached object
        is an object that has a database identity (i.e. a primary key)
        but is not associated with any session.  An object that
        was previously :term:`persistent` and was removed from its
        session either because it was expunged, or the owning
        session was closed, moves into the detached state.
        The detached state is generally used when objects are being
        moved between sessions or when being moved to/from an external
        object cache.

        .. seealso::

            :ref:`session_object_states`
