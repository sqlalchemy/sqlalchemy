/*
processors.c
Copyright (C) 2010 Gaetan de Menten gdementen@gmail.com

This module is part of SQLAlchemy and is released under
the MIT License: http://www.opensource.org/licenses/mit-license.php
*/

#include <Python.h>
#include <datetime.h>

#if PY_VERSION_HEX < 0x02050000 && !defined(PY_SSIZE_T_MIN)
typedef int Py_ssize_t;
#define PY_SSIZE_T_MAX INT_MAX
#define PY_SSIZE_T_MIN INT_MIN
#endif


static PyObject *
int_to_boolean(PyObject *self, PyObject *arg)
{
    long l = 0;
    PyObject *res;

    if (arg == Py_None)
        Py_RETURN_NONE;

    l = PyInt_AsLong(arg);
    if (l == 0) {
        res = Py_False;
    } else if (l == 1) {
        res = Py_True;
    } else if ((l == -1) && PyErr_Occurred()) {
        /* -1 can be either the actual value, or an error flag. */
        return NULL;
    } else {
        PyErr_SetString(PyExc_ValueError,
                        "int_to_boolean only accepts None, 0 or 1");
        return NULL;
    }

    Py_INCREF(res);
    return res;
}

static PyObject *
to_str(PyObject *self, PyObject *arg)
{
    if (arg == Py_None)
        Py_RETURN_NONE;

    return PyObject_Str(arg);
}

static PyObject *
to_float(PyObject *self, PyObject *arg)
{
    if (arg == Py_None)
        Py_RETURN_NONE;

    return PyNumber_Float(arg);
}

static PyObject *
str_to_datetime(PyObject *self, PyObject *arg)
{
    const char *str;
    unsigned int year, month, day, hour, minute, second, microsecond = 0;

    if (arg == Py_None)
        Py_RETURN_NONE;

    str = PyString_AsString(arg);
    if (str == NULL)
        return NULL;

    /* microseconds are optional */
    /*
    TODO: this is slightly less picky than the Python version which would
    not accept "2000-01-01 00:00:00.". I don't know which is better, but they
    should be coherent.
    */
    if (sscanf(str, "%4u-%2u-%2u %2u:%2u:%2u.%6u", &year, &month, &day,
               &hour, &minute, &second, &microsecond) < 6) {
        PyErr_SetString(PyExc_ValueError, "Couldn't parse datetime string.");
        return NULL;
    }
    return PyDateTime_FromDateAndTime(year, month, day,
                                      hour, minute, second, microsecond);
}

static PyObject *
str_to_time(PyObject *self, PyObject *arg)
{
    const char *str;
    unsigned int hour, minute, second, microsecond = 0;

    if (arg == Py_None)
        Py_RETURN_NONE;

    str = PyString_AsString(arg);
    if (str == NULL)
        return NULL;

    /* microseconds are optional */
    /*
    TODO: this is slightly less picky than the Python version which would
    not accept "00:00:00.". I don't know which is better, but they should be
    coherent.
    */
    if (sscanf(str, "%2u:%2u:%2u.%6u", &hour, &minute, &second,
               &microsecond) < 3) {
        PyErr_SetString(PyExc_ValueError, "Couldn't parse time string.");
        return NULL;
    }
    return PyTime_FromTime(hour, minute, second, microsecond);
}

static PyObject *
str_to_date(PyObject *self, PyObject *arg)
{
    const char *str;
    unsigned int year, month, day;

    if (arg == Py_None)
        Py_RETURN_NONE;

    str = PyString_AsString(arg);
    if (str == NULL)
        return NULL;

    if (sscanf(str, "%4u-%2u-%2u", &year, &month, &day) != 3) {
        PyErr_SetString(PyExc_ValueError, "Couldn't parse date string.");
        return NULL;
    }
    return PyDate_FromDate(year, month, day);
}


/***********
 * Structs *
 ***********/

typedef struct {
    PyObject_HEAD
    PyObject *encoding;
    PyObject *errors;
} UnicodeResultProcessor;

typedef struct {
    PyObject_HEAD
    PyObject *type;
    PyObject *format;
} DecimalResultProcessor;



/**************************
 * UnicodeResultProcessor *
 **************************/

static int
UnicodeResultProcessor_init(UnicodeResultProcessor *self, PyObject *args,
                            PyObject *kwds)
{
    PyObject *encoding, *errors = NULL;
    static char *kwlist[] = {"encoding", "errors", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "S|S:__init__", kwlist,
                                     &encoding, &errors))
        return -1;

    Py_INCREF(encoding);
    self->encoding = encoding;

    if (errors) {
        Py_INCREF(errors);
    } else {
        errors = PyString_FromString("strict");
        if (errors == NULL)
            return -1;
    }
    self->errors = errors;

    return 0;
}

static PyObject *
UnicodeResultProcessor_process(UnicodeResultProcessor *self, PyObject *value)
{
    const char *encoding, *errors;
    char *str;
    Py_ssize_t len;

    if (value == Py_None)
        Py_RETURN_NONE;

    if (PyString_AsStringAndSize(value, &str, &len))
        return NULL;

    encoding = PyString_AS_STRING(self->encoding);
    errors = PyString_AS_STRING(self->errors);

    return PyUnicode_Decode(str, len, encoding, errors);
}

static void
UnicodeResultProcessor_dealloc(UnicodeResultProcessor *self)
{
    Py_XDECREF(self->encoding);
    Py_XDECREF(self->errors);
    self->ob_type->tp_free((PyObject*)self);
}

static PyMethodDef UnicodeResultProcessor_methods[] = {
    {"process", (PyCFunction)UnicodeResultProcessor_process, METH_O,
     "The value processor itself."},
    {NULL}  /* Sentinel */
};

static PyTypeObject UnicodeResultProcessorType = {
    PyObject_HEAD_INIT(NULL)
    0,                                          /* ob_size */
    "sqlalchemy.cprocessors.UnicodeResultProcessor",        /* tp_name */
    sizeof(UnicodeResultProcessor),             /* tp_basicsize */
    0,                                          /* tp_itemsize */
    (destructor)UnicodeResultProcessor_dealloc, /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_compare */
    0,                                          /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash  */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    0,                                          /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,   /* tp_flags */
    "UnicodeResultProcessor objects",           /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    UnicodeResultProcessor_methods,             /* tp_methods */
    0,                                          /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    (initproc)UnicodeResultProcessor_init,      /* tp_init */
    0,                                          /* tp_alloc */
    0,                                          /* tp_new */
};

/**************************
 * DecimalResultProcessor *
 **************************/

static int
DecimalResultProcessor_init(DecimalResultProcessor *self, PyObject *args,
                            PyObject *kwds)
{
    PyObject *type, *format;

    if (!PyArg_ParseTuple(args, "OS", &type, &format))
        return -1;

    Py_INCREF(type);
    self->type = type;

    Py_INCREF(format);
    self->format = format;

    return 0;
}

static PyObject *
DecimalResultProcessor_process(DecimalResultProcessor *self, PyObject *value)
{
    PyObject *str, *result, *args;

    if (value == Py_None)
        Py_RETURN_NONE;

    if (PyFloat_CheckExact(value)) {
        /* Decimal does not accept float values directly */
        args = PyTuple_Pack(1, value);
        if (args == NULL)
            return NULL;

        str = PyString_Format(self->format, args);
        Py_DECREF(args);
        if (str == NULL)
            return NULL;

        result = PyObject_CallFunctionObjArgs(self->type, str, NULL);
        Py_DECREF(str);
        return result;
    } else {
        return PyObject_CallFunctionObjArgs(self->type, value, NULL);
    }
}

static void
DecimalResultProcessor_dealloc(DecimalResultProcessor *self)
{
    Py_XDECREF(self->type);
    Py_XDECREF(self->format);
    self->ob_type->tp_free((PyObject*)self);
}

static PyMethodDef DecimalResultProcessor_methods[] = {
    {"process", (PyCFunction)DecimalResultProcessor_process, METH_O,
     "The value processor itself."},
    {NULL}  /* Sentinel */
};

static PyTypeObject DecimalResultProcessorType = {
    PyObject_HEAD_INIT(NULL)
    0,                                          /* ob_size */
    "sqlalchemy.DecimalResultProcessor",        /* tp_name */
    sizeof(DecimalResultProcessor),             /* tp_basicsize */
    0,                                          /* tp_itemsize */
    (destructor)DecimalResultProcessor_dealloc, /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_compare */
    0,                                          /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash  */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    0,                                          /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,   /* tp_flags */
    "DecimalResultProcessor objects",           /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    DecimalResultProcessor_methods,             /* tp_methods */
    0,                                          /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    (initproc)DecimalResultProcessor_init,      /* tp_init */
    0,                                          /* tp_alloc */
    0,                                          /* tp_new */
};

#ifndef PyMODINIT_FUNC  /* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif


static PyMethodDef module_methods[] = {
    {"int_to_boolean", int_to_boolean, METH_O,
     "Convert an integer to a boolean."},
    {"to_str", to_str, METH_O,
     "Convert any value to its string representation."},
    {"to_float", to_float, METH_O,
     "Convert any value to its floating point representation."},
    {"str_to_datetime", str_to_datetime, METH_O,
     "Convert an ISO string to a datetime.datetime object."},
    {"str_to_time", str_to_time, METH_O,
     "Convert an ISO string to a datetime.time object."},
    {"str_to_date", str_to_date, METH_O,
     "Convert an ISO string to a datetime.date object."},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

PyMODINIT_FUNC
initcprocessors(void)
{
    PyObject *m;

    UnicodeResultProcessorType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&UnicodeResultProcessorType) < 0)
        return;

    DecimalResultProcessorType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&DecimalResultProcessorType) < 0)
        return;

    m = Py_InitModule3("cprocessors", module_methods,
                       "Module containing C versions of data processing functions.");
    if (m == NULL)
        return;

    PyDateTime_IMPORT;

    Py_INCREF(&UnicodeResultProcessorType);
    PyModule_AddObject(m, "UnicodeResultProcessor",
                       (PyObject *)&UnicodeResultProcessorType);

    Py_INCREF(&DecimalResultProcessorType);
    PyModule_AddObject(m, "DecimalResultProcessor",
                       (PyObject *)&DecimalResultProcessorType);
}

