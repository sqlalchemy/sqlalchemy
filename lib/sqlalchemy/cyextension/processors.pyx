import datetime
import re

from cpython.datetime cimport date_new, datetime_new, import_datetime, time_new
from cpython.object cimport PyObject_Str
from cpython.unicode cimport PyUnicode_AsASCIIString, PyUnicode_Check, PyUnicode_Decode
from libc.stdio cimport sscanf


def int_to_boolean(value):
    if value is None:
        return None
    return True if value else False

def to_str(value):
    return PyObject_Str(value) if value is not None else None

def to_float(value):
    return float(value) if value is not None else None

cdef inline bytes to_bytes(object value, str type_name):
    try:
        return PyUnicode_AsASCIIString(value)
    except Exception as e:
        raise ValueError(
            f"Couldn't parse {type_name} string '{value!r}' "
            "- value is not a string."
        ) from e

import_datetime()  # required to call datetime_new/date_new/time_new

def str_to_datetime(value):
    if value is None:
        return None
    cdef int numparsed
    cdef unsigned int year, month, day, hour, minute, second, microsecond = 0
    cdef bytes value_b = to_bytes(value, 'datetime')
    cdef const char * string = value_b

    numparsed = sscanf(string, "%4u-%2u-%2u %2u:%2u:%2u.%6u",
        &year, &month, &day, &hour, &minute, &second, &microsecond)
    if numparsed < 6:
        raise ValueError(
                "Couldn't parse datetime string: '%s'" % (value)
            )
    return datetime_new(year, month, day, hour, minute, second, microsecond, None)

def str_to_date(value):
    if value is None:
        return None
    cdef int numparsed
    cdef unsigned int year, month, day
    cdef bytes value_b = to_bytes(value, 'date')
    cdef const char * string = value_b

    numparsed = sscanf(string, "%4u-%2u-%2u", &year, &month, &day)
    if numparsed != 3:
        raise ValueError(
                "Couldn't parse date string: '%s'" % (value)
            )
    return date_new(year, month, day)

def str_to_time(value):
    if value is None:
        return None
    cdef int numparsed
    cdef unsigned int hour, minute, second, microsecond = 0
    cdef bytes value_b = to_bytes(value, 'time')
    cdef const char * string = value_b

    numparsed = sscanf(string, "%2u:%2u:%2u.%6u", &hour, &minute, &second, &microsecond)
    if numparsed < 3:
        raise ValueError(
                "Couldn't parse time string: '%s'" % (value)
            )
    return time_new(hour, minute, second, microsecond, None)


cdef class DecimalResultProcessor:
    cdef object type_
    cdef str format_

    def __cinit__(self, type_, format_):
        self.type_ = type_
        self.format_ = format_

    def process(self, object value):
        if value is None:
            return None
        else:
            return self.type_(self.format_ % value)
