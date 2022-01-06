# sqlalchemy/processors.py
# Copyright (C) 2010-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
# Copyright (C) 2010 Gaetan de Menten gdementen@gmail.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

"""defines generic type conversion functions, as used in bind and result
processors.

They all share one common characteristic: None is passed through unchanged.

"""

import datetime
import re


def str_to_datetime_processor_factory(regexp, type_):
    rmatch = regexp.match
    # Even on python2.6 datetime.strptime is both slower than this code
    # and it does not support microseconds.
    has_named_groups = bool(regexp.groupindex)

    def process(value):
        if value is None:
            return None
        else:
            try:
                m = rmatch(value)
            except TypeError as err:
                raise ValueError(
                    "Couldn't parse %s string '%r' "
                    "- value is not a string." % (type_.__name__, value)
                ) from err
            if m is None:
                raise ValueError(
                    "Couldn't parse %s string: "
                    "'%s'" % (type_.__name__, value)
                )
            if has_named_groups:
                groups = m.groupdict(0)
                return type_(
                    **dict(
                        list(
                            zip(
                                iter(groups.keys()),
                                list(map(int, iter(groups.values()))),
                            )
                        )
                    )
                )
            else:
                return type_(*list(map(int, m.groups(0))))

    return process


def to_decimal_processor_factory(target_class, scale):
    fstring = "%%.%df" % scale

    def process(value):
        if value is None:
            return None
        else:
            return target_class(fstring % value)

    return process


def to_float(value):
    if value is None:
        return None
    else:
        return float(value)


def to_str(value):
    if value is None:
        return None
    else:
        return str(value)


def int_to_boolean(value):
    if value is None:
        return None
    else:
        return bool(value)


DATETIME_RE = re.compile(r"(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?")
TIME_RE = re.compile(r"(\d+):(\d+):(\d+)(?:\.(\d+))?")
DATE_RE = re.compile(r"(\d+)-(\d+)-(\d+)")

str_to_datetime = str_to_datetime_processor_factory(
    DATETIME_RE, datetime.datetime
)
str_to_time = str_to_datetime_processor_factory(TIME_RE, datetime.time)
str_to_date = str_to_datetime_processor_factory(DATE_RE, datetime.date)
