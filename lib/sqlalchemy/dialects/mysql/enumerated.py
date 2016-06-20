# mysql/enumerated.py
# Copyright (C) 2005-2016 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import re

from .types import _StringType
from ... import exc, sql, util
from ... import types as sqltypes


class _EnumeratedValues(_StringType):
    def _init_values(self, values, kw):
        self.quoting = kw.pop('quoting', 'auto')

        if self.quoting == 'auto' and len(values):
            # What quoting character are we using?
            q = None
            for e in values:
                if len(e) == 0:
                    self.quoting = 'unquoted'
                    break
                elif q is None:
                    q = e[0]

                if len(e) == 1 or e[0] != q or e[-1] != q:
                    self.quoting = 'unquoted'
                    break
            else:
                self.quoting = 'quoted'

        if self.quoting == 'quoted':
            util.warn_deprecated(
                'Manually quoting %s value literals is deprecated.  Supply '
                'unquoted values and use the quoting= option in cases of '
                'ambiguity.' % self.__class__.__name__)

            values = self._strip_values(values)

        self._enumerated_values = values
        length = max([len(v) for v in values] + [0])
        return values, length

    @classmethod
    def _strip_values(cls, values):
        strip_values = []
        for a in values:
            if a[0:1] == '"' or a[0:1] == "'":
                # strip enclosing quotes and unquote interior
                a = a[1:-1].replace(a[0] * 2, a[0])
            strip_values.append(a)
        return strip_values


class ENUM(sqltypes.Enum, _EnumeratedValues):
    """MySQL ENUM type."""

    __visit_name__ = 'ENUM'

    def __init__(self, *enums, **kw):
        """Construct an ENUM.

        E.g.::

          Column('myenum', ENUM("foo", "bar", "baz"))

        :param enums: The range of valid values for this ENUM.  Values will be
          quoted when generating the schema according to the quoting flag (see
          below).  This object may also be a PEP-435-compliant enumerated
          type.

          .. versionadded: 1.1 added support for PEP-435-compliant enumerated
             types.

        :param strict: This flag has no effect.

         .. versionchanged:: The MySQL ENUM type as well as the base Enum
            type now validates all Python data values.

        :param charset: Optional, a column-level character set for this string
          value.  Takes precedence to 'ascii' or 'unicode' short-hand.

        :param collation: Optional, a column-level collation for this string
          value.  Takes precedence to 'binary' short-hand.

        :param ascii: Defaults to False: short-hand for the ``latin1``
          character set, generates ASCII in schema.

        :param unicode: Defaults to False: short-hand for the ``ucs2``
          character set, generates UNICODE in schema.

        :param binary: Defaults to False: short-hand, pick the binary
          collation type that matches the column's character set.  Generates
          BINARY in schema.  This does not affect the type of data stored,
          only the collation of character data.

        :param quoting: Defaults to 'auto': automatically determine enum value
          quoting.  If all enum values are surrounded by the same quoting
          character, then use 'quoted' mode.  Otherwise, use 'unquoted' mode.

          'quoted': values in enums are already quoted, they will be used
          directly when generating the schema - this usage is deprecated.

          'unquoted': values in enums are not quoted, they will be escaped and
          surrounded by single quotes when generating the schema.

          Previous versions of this type always required manually quoted
          values to be supplied; future versions will always quote the string
          literals for you.  This is a transitional option.

        """

        kw.pop('strict', None)
        validate_strings = kw.pop("validate_strings", False)
        sqltypes.Enum.__init__(
            self, validate_strings=validate_strings, *enums)
        kw.pop('metadata', None)
        kw.pop('schema', None)
        kw.pop('name', None)
        kw.pop('quote', None)
        kw.pop('native_enum', None)
        kw.pop('inherit_schema', None)
        kw.pop('_create_events', None)
        _StringType.__init__(self, length=self.length, **kw)

    def _setup_for_values(self, values, objects, kw):
        values, length = self._init_values(values, kw)
        return sqltypes.Enum._setup_for_values(self, values, objects, kw)

    def __repr__(self):
        return util.generic_repr(
            self, to_inspect=[ENUM, _StringType, sqltypes.Enum])

    def adapt(self, cls, **kw):
        return sqltypes.Enum.adapt(self, cls, **kw)


class SET(_EnumeratedValues):
    """MySQL SET type."""

    __visit_name__ = 'SET'

    def __init__(self, *values, **kw):
        """Construct a SET.

        E.g.::

          Column('myset', SET("foo", "bar", "baz"))


        The list of potential values is required in the case that this
        set will be used to generate DDL for a table, or if the
        :paramref:`.SET.retrieve_as_bitwise` flag is set to True.

        :param values: The range of valid values for this SET.

        :param convert_unicode: Same flag as that of
         :paramref:`.String.convert_unicode`.

        :param collation: same as that of :paramref:`.String.collation`

        :param charset: same as that of :paramref:`.VARCHAR.charset`.

        :param ascii: same as that of :paramref:`.VARCHAR.ascii`.

        :param unicode: same as that of :paramref:`.VARCHAR.unicode`.

        :param binary: same as that of :paramref:`.VARCHAR.binary`.

        :param quoting: Defaults to 'auto': automatically determine set value
          quoting.  If all values are surrounded by the same quoting
          character, then use 'quoted' mode.  Otherwise, use 'unquoted' mode.

          'quoted': values in enums are already quoted, they will be used
          directly when generating the schema - this usage is deprecated.

          'unquoted': values in enums are not quoted, they will be escaped and
          surrounded by single quotes when generating the schema.

          Previous versions of this type always required manually quoted
          values to be supplied; future versions will always quote the string
          literals for you.  This is a transitional option.

          .. versionadded:: 0.9.0

        :param retrieve_as_bitwise: if True, the data for the set type will be
          persisted and selected using an integer value, where a set is coerced
          into a bitwise mask for persistence.  MySQL allows this mode which
          has the advantage of being able to store values unambiguously,
          such as the blank string ``''``.   The datatype will appear
          as the expression ``col + 0`` in a SELECT statement, so that the
          value is coerced into an integer value in result sets.
          This flag is required if one wishes
          to persist a set that can store the blank string ``''`` as a value.

          .. warning::

            When using :paramref:`.mysql.SET.retrieve_as_bitwise`, it is
            essential that the list of set values is expressed in the
            **exact same order** as exists on the MySQL database.

          .. versionadded:: 1.0.0


        """
        self.retrieve_as_bitwise = kw.pop('retrieve_as_bitwise', False)
        values, length = self._init_values(values, kw)
        self.values = tuple(values)
        if not self.retrieve_as_bitwise and '' in values:
            raise exc.ArgumentError(
                "Can't use the blank value '' in a SET without "
                "setting retrieve_as_bitwise=True")
        if self.retrieve_as_bitwise:
            self._bitmap = dict(
                (value, 2 ** idx)
                for idx, value in enumerate(self.values)
            )
            self._bitmap.update(
                (2 ** idx, value)
                for idx, value in enumerate(self.values)
            )
        kw.setdefault('length', length)
        super(SET, self).__init__(**kw)

    def column_expression(self, colexpr):
        if self.retrieve_as_bitwise:
            return sql.type_coerce(
                sql.type_coerce(colexpr, sqltypes.Integer) + 0,
                self
            )
        else:
            return colexpr

    def result_processor(self, dialect, coltype):
        if self.retrieve_as_bitwise:
            def process(value):
                if value is not None:
                    value = int(value)

                    return set(
                        util.map_bits(self._bitmap.__getitem__, value)
                    )
                else:
                    return None
        else:
            super_convert = super(SET, self).result_processor(dialect, coltype)

            def process(value):
                if isinstance(value, util.string_types):
                    # MySQLdb returns a string, let's parse
                    if super_convert:
                        value = super_convert(value)
                    return set(re.findall(r'[^,]+', value))
                else:
                    # mysql-connector-python does a naive
                    # split(",") which throws in an empty string
                    if value is not None:
                        value.discard('')
                    return value
        return process

    def bind_processor(self, dialect):
        super_convert = super(SET, self).bind_processor(dialect)
        if self.retrieve_as_bitwise:
            def process(value):
                if value is None:
                    return None
                elif isinstance(value, util.int_types + util.string_types):
                    if super_convert:
                        return super_convert(value)
                    else:
                        return value
                else:
                    int_value = 0
                    for v in value:
                        int_value |= self._bitmap[v]
                    return int_value
        else:

            def process(value):
                # accept strings and int (actually bitflag) values directly
                if value is not None and not isinstance(
                        value, util.int_types + util.string_types):
                    value = ",".join(value)

                if super_convert:
                    return super_convert(value)
                else:
                    return value
        return process

    def adapt(self, impltype, **kw):
        kw['retrieve_as_bitwise'] = self.retrieve_as_bitwise
        return util.constructor_copy(
            self, impltype,
            *self.values,
            **kw
        )
