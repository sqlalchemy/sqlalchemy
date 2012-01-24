/*
resultproxy.c
Copyright (C) 2010 Gaetan de Menten gdementen@gmail.com

This module is part of SQLAlchemy and is released under
the MIT License: http://www.opensource.org/licenses/mit-license.php
*/

#include <Python.h>

#if PY_VERSION_HEX < 0x02050000 && !defined(PY_SSIZE_T_MIN)
typedef int Py_ssize_t;
#define PY_SSIZE_T_MAX INT_MAX
#define PY_SSIZE_T_MIN INT_MIN
typedef Py_ssize_t (*lenfunc)(PyObject *);
#define PyInt_FromSsize_t(x) PyInt_FromLong(x) 
typedef intargfunc ssizeargfunc; 
#endif


/***********
 * Structs *
 ***********/

typedef struct {
    PyObject_HEAD
    PyObject *parent;
    PyObject *row;
    PyObject *processors;
    PyObject *keymap;
} BaseRowProxy;

/****************
 * BaseRowProxy *
 ****************/

static PyObject *
safe_rowproxy_reconstructor(PyObject *self, PyObject *args)
{
    PyObject *cls, *state, *tmp;
    BaseRowProxy *obj;

    if (!PyArg_ParseTuple(args, "OO", &cls, &state))
        return NULL;

    obj = (BaseRowProxy *)PyObject_CallMethod(cls, "__new__", "O", cls);
    if (obj == NULL)
        return NULL;

    tmp = PyObject_CallMethod((PyObject *)obj, "__setstate__", "O", state);
    if (tmp == NULL) {
        Py_DECREF(obj);
        return NULL;
    }
    Py_DECREF(tmp);

    if (obj->parent == NULL || obj->row == NULL ||
        obj->processors == NULL || obj->keymap == NULL) {
        PyErr_SetString(PyExc_RuntimeError,
            "__setstate__ for BaseRowProxy subclasses must set values "
            "for parent, row, processors and keymap");
        Py_DECREF(obj);
        return NULL;
    }

    return (PyObject *)obj;
}

static int
BaseRowProxy_init(BaseRowProxy *self, PyObject *args, PyObject *kwds)
{
    PyObject *parent, *row, *processors, *keymap;

    if (!PyArg_UnpackTuple(args, "BaseRowProxy", 4, 4,
                           &parent, &row, &processors, &keymap))
        return -1;

    Py_INCREF(parent);
    self->parent = parent;

    if (!PySequence_Check(row)) {
        PyErr_SetString(PyExc_TypeError, "row must be a sequence");
        return -1;
    }
    Py_INCREF(row);
    self->row = row;

    if (!PyList_CheckExact(processors)) {
        PyErr_SetString(PyExc_TypeError, "processors must be a list");
        return -1;
    }
    Py_INCREF(processors);
    self->processors = processors;

    if (!PyDict_CheckExact(keymap)) {
        PyErr_SetString(PyExc_TypeError, "keymap must be a dict");
        return -1;
    }
    Py_INCREF(keymap);
    self->keymap = keymap;

    return 0;
}

/* We need the reduce method because otherwise the default implementation
 * does very weird stuff for pickle protocol 0 and 1. It calls
 * BaseRowProxy.__new__(RowProxy_instance) upon *pickling*.
 */
static PyObject *
BaseRowProxy_reduce(PyObject *self)
{
    PyObject *method, *state;
    PyObject *module, *reconstructor, *cls;

    method = PyObject_GetAttrString(self, "__getstate__");
    if (method == NULL)
        return NULL;

    state = PyObject_CallObject(method, NULL);
    Py_DECREF(method);
    if (state == NULL)
        return NULL;

    module = PyImport_ImportModule("sqlalchemy.engine.base");
    if (module == NULL)
        return NULL;

    reconstructor = PyObject_GetAttrString(module, "rowproxy_reconstructor");
    Py_DECREF(module);
    if (reconstructor == NULL) {
        Py_DECREF(state);
        return NULL;
    }

    cls = PyObject_GetAttrString(self, "__class__");
    if (cls == NULL) {
        Py_DECREF(reconstructor);
        Py_DECREF(state);
        return NULL;
    }

    return Py_BuildValue("(N(NN))", reconstructor, cls, state);
}

static void
BaseRowProxy_dealloc(BaseRowProxy *self)
{
    Py_XDECREF(self->parent);
    Py_XDECREF(self->row);
    Py_XDECREF(self->processors);
    Py_XDECREF(self->keymap);
    self->ob_type->tp_free((PyObject *)self);
}

static PyObject *
BaseRowProxy_processvalues(PyObject *values, PyObject *processors, int astuple)
{
    Py_ssize_t num_values, num_processors;
    PyObject **valueptr, **funcptr, **resultptr;
    PyObject *func, *result, *processed_value, *values_fastseq;

    num_values = PySequence_Length(values);
    num_processors = PyList_Size(processors);
    if (num_values != num_processors) {
        PyErr_Format(PyExc_RuntimeError,
            "number of values in row (%d) differ from number of column "
            "processors (%d)",
            (int)num_values, (int)num_processors);
        return NULL;
    }

    if (astuple) {
        result = PyTuple_New(num_values);
    } else {
        result = PyList_New(num_values);
    }
    if (result == NULL)
        return NULL;

    values_fastseq = PySequence_Fast(values, "row must be a sequence");
    if (values_fastseq == NULL)
        return NULL;

    valueptr = PySequence_Fast_ITEMS(values_fastseq);
    funcptr = PySequence_Fast_ITEMS(processors);
    resultptr = PySequence_Fast_ITEMS(result);
    while (--num_values >= 0) {
        func = *funcptr;
        if (func != Py_None) {
            processed_value = PyObject_CallFunctionObjArgs(func, *valueptr,
                                                           NULL);
            if (processed_value == NULL) {
                Py_DECREF(values_fastseq);
                Py_DECREF(result);
                return NULL;
            }
            *resultptr = processed_value;
        } else {
            Py_INCREF(*valueptr);
            *resultptr = *valueptr;
        }
        valueptr++;
        funcptr++;
        resultptr++;
    }
    Py_DECREF(values_fastseq);
    return result;
}

static PyListObject *
BaseRowProxy_values(BaseRowProxy *self)
{
    return (PyListObject *)BaseRowProxy_processvalues(self->row,
                                                      self->processors, 0);
}

static PyObject *
BaseRowProxy_iter(BaseRowProxy *self)
{
    PyObject *values, *result;

    values = BaseRowProxy_processvalues(self->row, self->processors, 1);
    if (values == NULL)
        return NULL;

    result = PyObject_GetIter(values);
    Py_DECREF(values);
    if (result == NULL)
        return NULL;

    return result;
}

static Py_ssize_t
BaseRowProxy_length(BaseRowProxy *self)
{
    return PySequence_Length(self->row);
}

static PyObject *
BaseRowProxy_subscript(BaseRowProxy *self, PyObject *key)
{
    PyObject *processors, *values;
    PyObject *processor, *value;
    PyObject *row, *record, *result, *indexobject;
    PyObject *exc_module, *exception;
    char *cstr_key;
    long index;

    if (PyInt_CheckExact(key)) {
        index = PyInt_AS_LONG(key);
    } else if (PyLong_CheckExact(key)) {
        index = PyLong_AsLong(key);
        if ((index == -1) && PyErr_Occurred())
            /* -1 can be either the actual value, or an error flag. */
            return NULL;
    } else if (PySlice_Check(key)) {
        values = PyObject_GetItem(self->row, key);
        if (values == NULL)
            return NULL;

        processors = PyObject_GetItem(self->processors, key);
        if (processors == NULL) {
            Py_DECREF(values);
            return NULL;
        }

        result = BaseRowProxy_processvalues(values, processors, 1);
        Py_DECREF(values);
        Py_DECREF(processors);
        return result;
    } else {
        record = PyDict_GetItem((PyObject *)self->keymap, key);
        if (record == NULL) {
            record = PyObject_CallMethod(self->parent, "_key_fallback",
                                         "O", key);
            if (record == NULL)
                return NULL;
        }

        indexobject = PyTuple_GetItem(record, 2);
        if (indexobject == NULL)
            return NULL;

        if (indexobject == Py_None) {
            exc_module = PyImport_ImportModule("sqlalchemy.exc");
            if (exc_module == NULL)
                return NULL;

            exception = PyObject_GetAttrString(exc_module,
                                               "InvalidRequestError");
            Py_DECREF(exc_module);
            if (exception == NULL)
                return NULL;

            cstr_key = PyString_AsString(key);
            if (cstr_key == NULL)
                return NULL;

            PyErr_Format(exception,
                    "Ambiguous column name '%.200s' in result set! "
                    "try 'use_labels' option on select statement.", cstr_key);
            return NULL;
        }

        index = PyInt_AsLong(indexobject);
        if ((index == -1) && PyErr_Occurred())
            /* -1 can be either the actual value, or an error flag. */
            return NULL;
    }
    processor = PyList_GetItem(self->processors, index);
    if (processor == NULL)
        return NULL;

    row = self->row;
    if (PyTuple_CheckExact(row))
        value = PyTuple_GetItem(row, index);
    else
        value = PySequence_GetItem(row, index);
    if (value == NULL)
        return NULL;

    if (processor != Py_None) {
        return PyObject_CallFunctionObjArgs(processor, value, NULL);
    } else {
        Py_INCREF(value);
        return value;
    }
}

static PyObject *
BaseRowProxy_getitem(PyObject *self, Py_ssize_t i)
{
    return BaseRowProxy_subscript((BaseRowProxy*)self, PyInt_FromSsize_t(i));
}

static PyObject *
BaseRowProxy_getattro(BaseRowProxy *self, PyObject *name)
{
    PyObject *tmp;

    if (!(tmp = PyObject_GenericGetAttr((PyObject *)self, name))) {
        if (!PyErr_ExceptionMatches(PyExc_AttributeError))
            return NULL;
        PyErr_Clear();
    }
    else
        return tmp;

    return BaseRowProxy_subscript(self, name);
}

/***********************
 * getters and setters *
 ***********************/

static PyObject *
BaseRowProxy_getparent(BaseRowProxy *self, void *closure)
{
    Py_INCREF(self->parent);
    return self->parent;
}

static int
BaseRowProxy_setparent(BaseRowProxy *self, PyObject *value, void *closure)
{
    PyObject *module, *cls;

    if (value == NULL) {
        PyErr_SetString(PyExc_TypeError,
                        "Cannot delete the 'parent' attribute");
        return -1;
    }

    module = PyImport_ImportModule("sqlalchemy.engine.base");
    if (module == NULL)
        return -1;

    cls = PyObject_GetAttrString(module, "ResultMetaData");
    Py_DECREF(module);
    if (cls == NULL)
        return -1;

    if (PyObject_IsInstance(value, cls) != 1) {
        PyErr_SetString(PyExc_TypeError,
                        "The 'parent' attribute value must be an instance of "
                        "ResultMetaData");
        return -1;
    }
    Py_DECREF(cls);
    Py_XDECREF(self->parent);
    Py_INCREF(value);
    self->parent = value;

    return 0;
}

static PyObject *
BaseRowProxy_getrow(BaseRowProxy *self, void *closure)
{
    Py_INCREF(self->row);
    return self->row;
}

static int
BaseRowProxy_setrow(BaseRowProxy *self, PyObject *value, void *closure)
{
    if (value == NULL) {
        PyErr_SetString(PyExc_TypeError,
                        "Cannot delete the 'row' attribute");
        return -1;
    }

    if (!PySequence_Check(value)) {
        PyErr_SetString(PyExc_TypeError,
                        "The 'row' attribute value must be a sequence");
        return -1;
    }

    Py_XDECREF(self->row);
    Py_INCREF(value);
    self->row = value;

    return 0;
}

static PyObject *
BaseRowProxy_getprocessors(BaseRowProxy *self, void *closure)
{
    Py_INCREF(self->processors);
    return self->processors;
}

static int
BaseRowProxy_setprocessors(BaseRowProxy *self, PyObject *value, void *closure)
{
    if (value == NULL) {
        PyErr_SetString(PyExc_TypeError,
                        "Cannot delete the 'processors' attribute");
        return -1;
    }

    if (!PyList_CheckExact(value)) {
        PyErr_SetString(PyExc_TypeError,
                        "The 'processors' attribute value must be a list");
        return -1;
    }

    Py_XDECREF(self->processors);
    Py_INCREF(value);
    self->processors = value;

    return 0;
}

static PyObject *
BaseRowProxy_getkeymap(BaseRowProxy *self, void *closure)
{
    Py_INCREF(self->keymap);
    return self->keymap;
}

static int
BaseRowProxy_setkeymap(BaseRowProxy *self, PyObject *value, void *closure)
{
    if (value == NULL) {
        PyErr_SetString(PyExc_TypeError,
                        "Cannot delete the 'keymap' attribute");
        return -1;
    }

    if (!PyDict_CheckExact(value)) {
        PyErr_SetString(PyExc_TypeError,
                        "The 'keymap' attribute value must be a dict");
        return -1;
    }

    Py_XDECREF(self->keymap);
    Py_INCREF(value);
    self->keymap = value;

    return 0;
}

static PyGetSetDef BaseRowProxy_getseters[] = {
    {"_parent",
     (getter)BaseRowProxy_getparent, (setter)BaseRowProxy_setparent,
     "ResultMetaData",
     NULL},
    {"_row",
     (getter)BaseRowProxy_getrow, (setter)BaseRowProxy_setrow,
     "Original row tuple",
     NULL},
    {"_processors",
     (getter)BaseRowProxy_getprocessors, (setter)BaseRowProxy_setprocessors,
     "list of type processors",
     NULL},
    {"_keymap",
     (getter)BaseRowProxy_getkeymap, (setter)BaseRowProxy_setkeymap,
     "Key to (processor, index) dict",
     NULL},
    {NULL}
};

static PyMethodDef BaseRowProxy_methods[] = {
    {"values", (PyCFunction)BaseRowProxy_values, METH_NOARGS,
     "Return the values represented by this BaseRowProxy as a list."},
    {"__reduce__",  (PyCFunction)BaseRowProxy_reduce, METH_NOARGS,
     "Pickle support method."},
    {NULL}  /* Sentinel */
};

static PySequenceMethods BaseRowProxy_as_sequence = {
    (lenfunc)BaseRowProxy_length,   /* sq_length */
    0,                              /* sq_concat */
    0,                              /* sq_repeat */
    (ssizeargfunc)BaseRowProxy_getitem,          /* sq_item */
    0,                              /* sq_slice */
    0,                              /* sq_ass_item */
    0,                              /* sq_ass_slice */
    0,                              /* sq_contains */
    0,                              /* sq_inplace_concat */
    0,                              /* sq_inplace_repeat */
};

static PyMappingMethods BaseRowProxy_as_mapping = {
    (lenfunc)BaseRowProxy_length,       /* mp_length */
    (binaryfunc)BaseRowProxy_subscript, /* mp_subscript */
    0                                   /* mp_ass_subscript */
};

static PyTypeObject BaseRowProxyType = {
    PyObject_HEAD_INIT(NULL)
    0,                                  /* ob_size */
    "sqlalchemy.cresultproxy.BaseRowProxy",          /* tp_name */
    sizeof(BaseRowProxy),               /* tp_basicsize */
    0,                                  /* tp_itemsize */
    (destructor)BaseRowProxy_dealloc,   /* tp_dealloc */
    0,                                  /* tp_print */
    0,                                  /* tp_getattr */
    0,                                  /* tp_setattr */
    0,                                  /* tp_compare */
    0,                                  /* tp_repr */
    0,                                  /* tp_as_number */
    &BaseRowProxy_as_sequence,          /* tp_as_sequence */
    &BaseRowProxy_as_mapping,           /* tp_as_mapping */
    0,                                  /* tp_hash */
    0,                                  /* tp_call */
    0,                                  /* tp_str */
    (getattrofunc)BaseRowProxy_getattro,/* tp_getattro */
    0,                                  /* tp_setattro */
    0,                                  /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,               /* tp_flags */
    "BaseRowProxy is a abstract base class for RowProxy",   /* tp_doc */
    0,                                  /* tp_traverse */
    0,                                  /* tp_clear */
    0,                                  /* tp_richcompare */
    0,                                  /* tp_weaklistoffset */
    (getiterfunc)BaseRowProxy_iter,     /* tp_iter */
    0,                                  /* tp_iternext */
    BaseRowProxy_methods,               /* tp_methods */
    0,                                  /* tp_members */
    BaseRowProxy_getseters,             /* tp_getset */
    0,                                  /* tp_base */
    0,                                  /* tp_dict */
    0,                                  /* tp_descr_get */
    0,                                  /* tp_descr_set */
    0,                                  /* tp_dictoffset */
    (initproc)BaseRowProxy_init,        /* tp_init */
    0,                                  /* tp_alloc */
    0                                   /* tp_new */
};


#ifndef PyMODINIT_FUNC  /* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif


static PyMethodDef module_methods[] = {
    {"safe_rowproxy_reconstructor", safe_rowproxy_reconstructor, METH_VARARGS,
     "reconstruct a RowProxy instance from its pickled form."},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

PyMODINIT_FUNC
initcresultproxy(void)
{
    PyObject *m;

    BaseRowProxyType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&BaseRowProxyType) < 0)
        return;

    m = Py_InitModule3("cresultproxy", module_methods,
                       "Module containing C versions of core ResultProxy classes.");
    if (m == NULL)
        return;

    Py_INCREF(&BaseRowProxyType);
    PyModule_AddObject(m, "BaseRowProxy", (PyObject *)&BaseRowProxyType);

}

