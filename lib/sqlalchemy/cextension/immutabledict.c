/*
immuatbledict.c
Copyright (C) 2005-2021 the SQLAlchemy authors and contributors <see AUTHORS file>

This module is part of SQLAlchemy and is released under
the MIT License: http://www.opensource.org/licenses/mit-license.php
*/

#include <Python.h>

#define MODULE_NAME "cimmutabledict"
#define MODULE_DOC "immutable dictionary implementation"


typedef struct {
    PyObject_HEAD
    PyObject *dict;
} ImmutableDict;

static PyTypeObject ImmutableDictType;


#if PY_MAJOR_VERSION < 3
/* For Python 2.7, VENDORED from cPython: https://github.com/python/cpython/commit/1c496178d2c863f135bd4a43e32e0f099480cd06
   This function was added to Python 2.7.12 as an underscore function.

   Variant of PyDict_GetItem() that doesn't suppress exceptions.
   This returns NULL *with* an exception set if an exception occurred.
   It returns NULL *without* an exception set if the key wasn't present.
*/
PyObject *
PyDict_GetItemWithError(PyObject *op, PyObject *key)
{
    long hash;
    PyDictObject *mp = (PyDictObject *)op;
    PyDictEntry *ep;
    if (!PyDict_Check(op)) {
        PyErr_BadInternalCall();
        return NULL;
    }
    if (!PyString_CheckExact(key) ||
        (hash = ((PyStringObject *) key)->ob_shash) == -1)
    {
        hash = PyObject_Hash(key);
        if (hash == -1) {
            return NULL;
        }
    }

    ep = (mp->ma_lookup)(mp, key, hash);
    if (ep == NULL) {
        return NULL;
    }
    return ep->me_value;
}
#endif

static PyObject *

ImmutableDict_new(PyTypeObject *type, PyObject *args, PyObject *kw)

{
    ImmutableDict *new_obj;
    PyObject *arg_dict = NULL;
    PyObject *our_dict;

    if (!PyArg_UnpackTuple(args, "ImmutableDict", 0, 1, &arg_dict)) {
        return NULL;
    }

    if (arg_dict != NULL && PyDict_CheckExact(arg_dict)) {
        // going on the unproven theory that doing PyDict_New + PyDict_Update
        // is faster than just calling CallObject, as we do below to
        // accommodate for other dictionary argument forms
        our_dict = PyDict_New();
        if (our_dict == NULL) {
            return NULL;
        }

        if (PyDict_Update(our_dict, arg_dict) == -1) {
            Py_DECREF(our_dict);
            return NULL;
        }
    }
    else {
        // for other calling styles, let PyDict figure it out
        our_dict = PyObject_Call((PyObject *) &PyDict_Type, args, kw);
    }

    new_obj = PyObject_GC_New(ImmutableDict, &ImmutableDictType);
    if (new_obj == NULL) {
        Py_DECREF(our_dict);
        return NULL;
    }
    new_obj->dict = our_dict;
    PyObject_GC_Track(new_obj);

    return (PyObject *)new_obj;

}


Py_ssize_t
ImmutableDict_length(ImmutableDict *self)
{
    return PyDict_Size(self->dict);
}

static PyObject *
ImmutableDict_subscript(ImmutableDict *self, PyObject *key)
{
    PyObject *value;
#if PY_MAJOR_VERSION >= 3
    PyObject *err_bytes;
#endif

    value = PyDict_GetItemWithError(self->dict, key);

    if (value == NULL) {
        if (PyErr_Occurred() != NULL) {
            // there was an error while hashing the key
            return NULL;
        }
#if PY_MAJOR_VERSION >= 3
        err_bytes = PyUnicode_AsUTF8String(key);
        if (err_bytes == NULL)
            return NULL;
        PyErr_Format(PyExc_KeyError, "%s", PyBytes_AS_STRING(err_bytes));
#else
        PyErr_Format(PyExc_KeyError, "%s", PyString_AsString(key));
#endif
        return NULL;
    }

    Py_INCREF(value);

    return value;
}


static void
ImmutableDict_dealloc(ImmutableDict *self)
{
    PyObject_GC_UnTrack(self);
    Py_XDECREF(self->dict);
    PyObject_GC_Del(self);
}


static PyObject *
ImmutableDict_reduce(ImmutableDict *self)
{
    return Py_BuildValue("O(O)", Py_TYPE(self), self->dict);
}


static PyObject *
ImmutableDict_repr(ImmutableDict *self)
{
    return PyUnicode_FromFormat("immutabledict(%R)", self->dict);
}


static PyObject *
ImmutableDict_union(PyObject *self, PyObject *args, PyObject *kw)
{
    PyObject *arg_dict, *new_dict;

    ImmutableDict *new_obj;

    if (!PyArg_UnpackTuple(args, "ImmutableDict", 0, 1, &arg_dict)) {
        return NULL;
    }

    if (!PyDict_CheckExact(arg_dict)) {
        // if we didnt get a dict, and got lists of tuples or
        // keyword args, make a dict
        arg_dict = PyObject_Call((PyObject *) &PyDict_Type, args, kw);
        if (arg_dict == NULL) {
            return NULL;
        }
    }
    else {
        // otherwise we will use the dict as is
        Py_INCREF(arg_dict);
    }

    if (PyDict_Size(arg_dict) == 0) {
        Py_DECREF(arg_dict);
        Py_INCREF(self);
        return self;
    }

    new_dict = PyDict_New();
    if (new_dict == NULL) {
        Py_DECREF(arg_dict);
        return NULL;
    }

    if (PyDict_Update(new_dict, ((ImmutableDict *)self)->dict) == -1) {
        Py_DECREF(arg_dict);
        Py_DECREF(new_dict);
        return NULL;
    }

    if (PyDict_Update(new_dict, arg_dict) == -1) {
        Py_DECREF(arg_dict);
        Py_DECREF(new_dict);
        return NULL;
    }

    Py_DECREF(arg_dict);

    new_obj = PyObject_GC_New(ImmutableDict, Py_TYPE(self));
    if (new_obj == NULL) {
        Py_DECREF(new_dict);
        return NULL;
    }

    new_obj->dict = new_dict;

    PyObject_GC_Track(new_obj);

    return (PyObject *)new_obj;
}


static PyObject *
ImmutableDict_merge_with(PyObject *self, PyObject *args)
{
    PyObject *element, *arg, *new_dict = NULL;

    ImmutableDict *new_obj;

    Py_ssize_t num_args = PyTuple_Size(args);
    Py_ssize_t i;

    for (i=0; i<num_args; i++) {
        element = PyTuple_GetItem(args, i);

        if (element == NULL) {
            Py_XDECREF(new_dict);
            return NULL;
        }
        else if (element == Py_None) {
            // none was passed, skip it
            continue;
        }

        if (!PyDict_CheckExact(element)) {
            // not a dict, try to make a dict

            arg = PyTuple_Pack(1, element);

            element = PyObject_CallObject((PyObject *) &PyDict_Type, arg);

            Py_DECREF(arg);

            if (element == NULL) {
                Py_XDECREF(new_dict);
                return NULL;
            }
        }
        else {
            Py_INCREF(element);
            if (PyDict_Size(element) == 0) {
                continue;
            }
        }

        // initialize a new dictionary only if we receive data that
        // is not empty.  otherwise we return self at the end.
        if (new_dict == NULL) {

            new_dict = PyDict_New();
            if (new_dict == NULL) {
                Py_DECREF(element);
                return NULL;
            }

            if (PyDict_Update(new_dict, ((ImmutableDict *)self)->dict) == -1) {
                Py_DECREF(element);
                Py_DECREF(new_dict);
                return NULL;
            }
        }

        if (PyDict_Update(new_dict, element) == -1) {
            Py_DECREF(element);
            Py_DECREF(new_dict);
            return NULL;
        }

        Py_DECREF(element);
    }


    if (new_dict != NULL) {
        new_obj = PyObject_GC_New(ImmutableDict, Py_TYPE(self));
        if (new_obj == NULL) {
            Py_DECREF(new_dict);
            return NULL;
        }

        new_obj->dict = new_dict;
        PyObject_GC_Track(new_obj);
        return (PyObject *)new_obj;
    }
    else {
        Py_INCREF(self);
        return self;
    }

}


static PyObject *
ImmutableDict_get(ImmutableDict *self, PyObject *args)
{
    PyObject *key;
    PyObject *value;
    PyObject *default_value = Py_None;

    if (!PyArg_UnpackTuple(args, "key", 1, 2, &key, &default_value)) {
        return NULL;
    }

    value = PyDict_GetItemWithError(self->dict, key);

    if (value == NULL) {
        if (PyErr_Occurred() != NULL) {
            // there was an error while hashing the key
            return NULL;
        } else {
            // return default
            Py_INCREF(default_value);
            return default_value;
        }
    } else {
        Py_INCREF(value);
        return value;
    }
}

static PyObject *
ImmutableDict_keys(ImmutableDict *self)
{
    return PyDict_Keys(self->dict);
}

static int
ImmutableDict_traverse(ImmutableDict *self, visitproc visit, void *arg)
{
    Py_VISIT(self->dict);
    return 0;
}

static PyObject *
ImmutableDict_richcompare(ImmutableDict *self, PyObject *other, int op)
{
    return PyObject_RichCompare(self->dict, other, op);
}

static PyObject *
ImmutableDict_iter(ImmutableDict *self)
{
    return PyObject_GetIter(self->dict);
}

static PyObject *
ImmutableDict_items(ImmutableDict *self)
{
    return PyDict_Items(self->dict);
}

static PyObject *
ImmutableDict_values(ImmutableDict *self)
{
    return PyDict_Values(self->dict);
}

static PyObject *
ImmutableDict_contains(ImmutableDict *self, PyObject *key)
{
    int ret;

    ret = PyDict_Contains(self->dict, key);

    if (ret == 1) Py_RETURN_TRUE;
    else if (ret == 0) Py_RETURN_FALSE;
    else return NULL;
}

static PyMethodDef ImmutableDict_methods[] = {
    {"union", (PyCFunction) ImmutableDict_union, METH_VARARGS | METH_KEYWORDS,
     "provide a union of this dictionary with the given dictionary-like arguments"},
    {"merge_with", (PyCFunction) ImmutableDict_merge_with, METH_VARARGS,
     "provide a union of this dictionary with those given"},
    {"keys", (PyCFunction) ImmutableDict_keys, METH_NOARGS,
     "return dictionary keys"},

     {"__contains__",(PyCFunction)ImmutableDict_contains, METH_O,
     "test a member for containment"},

    {"items", (PyCFunction) ImmutableDict_items, METH_NOARGS,
     "return dictionary items"},
    {"values", (PyCFunction) ImmutableDict_values, METH_NOARGS,
     "return dictionary values"},
    {"get", (PyCFunction) ImmutableDict_get, METH_VARARGS,
     "get a value"},
    {"__reduce__",  (PyCFunction)ImmutableDict_reduce, METH_NOARGS,
     "Pickle support method."},
    {NULL},
};


static PyMappingMethods ImmutableDict_as_mapping = {
    (lenfunc)ImmutableDict_length,       /* mp_length */
    (binaryfunc)ImmutableDict_subscript, /* mp_subscript */
    0                                   /* mp_ass_subscript */
};




static PyTypeObject ImmutableDictType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "sqlalchemy.cimmutabledict.immutabledict",          /* tp_name */
    sizeof(ImmutableDict),               /* tp_basicsize */
    0,                                  /* tp_itemsize */
    (destructor)ImmutableDict_dealloc,  /* tp_dealloc */
    0,                                  /* tp_print */
    0,                                  /* tp_getattr */
    0,                                  /* tp_setattr */
    0,                                  /* tp_compare */
    (reprfunc)ImmutableDict_repr,               /* tp_repr */
    0,                                  /* tp_as_number */
    0,                                   /* tp_as_sequence */
    &ImmutableDict_as_mapping,            /* tp_as_mapping */
    0,                                 /* tp_hash */
    0,                                  /* tp_call */
    0,                                  /* tp_str */
    0,                                   /* tp_getattro */
    0,                                  /* tp_setattro */
    0,                                  /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC , /* tp_flags */
    "immutable dictionary",                         /* tp_doc */
    (traverseproc)ImmutableDict_traverse,          /* tp_traverse */
    0,                                  /* tp_clear */
    (richcmpfunc)ImmutableDict_richcompare, /* tp_richcompare */
    0,                                  /* tp_weaklistoffset */
    (getiterfunc)ImmutableDict_iter,     /* tp_iter */
    0,                                  /* tp_iternext */
    ImmutableDict_methods,               /* tp_methods */
    0,                                  /* tp_members */
    0,                                     /* tp_getset */
    0,                                  /* tp_base */
    0,                                  /* tp_dict */
    0,                                  /* tp_descr_get */
    0,                                  /* tp_descr_set */
    0,                                  /* tp_dictoffset */
    0,                                 /* tp_init */
    0,                                  /* tp_alloc */
    ImmutableDict_new,                   /* tp_new */
    0,                                    /* tp_free */
};





static PyMethodDef module_methods[] = {
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

#ifndef PyMODINIT_FUNC  /* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif


#if PY_MAJOR_VERSION >= 3

static struct PyModuleDef module_def = {
    PyModuleDef_HEAD_INIT,
    MODULE_NAME,
    MODULE_DOC,
    -1,
    module_methods
};

#define INITERROR return NULL

PyMODINIT_FUNC
PyInit_cimmutabledict(void)

#else

#define INITERROR return

PyMODINIT_FUNC
initcimmutabledict(void)

#endif

{
    PyObject *m;

    if (PyType_Ready(&ImmutableDictType) < 0)
        INITERROR;


#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&module_def);
#else
    m = Py_InitModule3(MODULE_NAME, module_methods, MODULE_DOC);
#endif
    if (m == NULL)
        INITERROR;

    Py_INCREF(&ImmutableDictType);
    PyModule_AddObject(m, "immutabledict", (PyObject *)&ImmutableDictType);

#if PY_MAJOR_VERSION >= 3
    return m;
#endif
}
