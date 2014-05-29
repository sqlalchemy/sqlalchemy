==============
1.0 Changelog
==============

.. changelog_imports::

    .. include:: changelog_09.rst
        :start-line: 5

    .. include:: changelog_08.rst
        :start-line: 5

    .. include:: changelog_07.rst
        :start-line: 5

.. changelog::
	:version: 1.0.0

    .. change::
        :tags: enhancement, orm
        :tickets: 3061

        Adjustment to attribute mechanics concerning when a value is
        implicitly initialized to None via first access; this action,
        which has always resulted in a population of the attribute,
        now emits an attribute event just like any other attribute set
        operation and generates the same kind of history as one.  Additionally,
        many mapper internal operations will no longer implicitly generate
        these "None" values when various never-set attributes are checked.
        These are subtle behavioral fixes to attribute mechanics which provide
        a better solution to the problem of :ticket:`3060`, which also
        involves recognition of attributes explicitly set to ``None``
        vs. attributes that were never set.

        .. seealso::

        	:ref:`migration_3061`

	.. change::
		:tags: feature, sql
		:tickets: 3034

		The :meth:`.Select.limit` and :meth:`.Select.offset` methods
		now accept any SQL expression, in addition to integer values, as
		arguments.  Typically this is used to allow a bound parameter to be
		passed, which can be substituted with a value later thus allowing
		Python-side caching of the SQL query.   The implementation
		here is fully backwards compatible with existing third party dialects,
		however those dialects which implement special LIMIT/OFFSET systems
		will need modification in order to take advantage of the new
		capabilities.  Work on this feature is courtesy of Dobes Vandermeer.

		.. seealso::

			:ref:`feature_3034`.
