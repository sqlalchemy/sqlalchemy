/*
utils.c
Copyright (C) 2012-2017 the SQLAlchemy authors and contributors <see AUTHORS file>

This module is part of SQLAlchemy and is released under
the MIT License: http://www.opensource.org/licenses/mit-license.php
*/

#include <Python.h>

#define MODULE_NAME "cutils"
#define MODULE_DOC "Module containing C versions of utility functions."

/*
    Given arguments from the calling form *multiparams, **params,
    return a list of bind parameter structures, usually a list of
    dictionaries.

    In the case of 'raw' execution which accepts positional parameters,
    it may be a list of tuples or lists.

 */
static PyObject *
distill_params(PyObject *self, PyObject *args)
{
	PyObject *multiparams, *params;
	PyObject *enclosing_list, *double_enclosing_list;
	PyObject *zero_element, *zero_element_item;
	Py_ssize_t multiparam_size, zero_element_length;

	if (!PyArg_UnpackTuple(args, "_distill_params", 2, 2, &multiparams, &params)) {
		return NULL;
	}

	if (multiparams != Py_None) {
		multiparam_size = PyTuple_Size(multiparams);
		if (multiparam_size < 0) {
			return NULL;
		}
	}
	else {
		multiparam_size = 0;
	}

	if (multiparam_size == 0) {
		if (params != Py_None && PyDict_Size(params) != 0) {
			enclosing_list = PyList_New(1);
			if (enclosing_list == NULL) {
				return NULL;
			}
			Py_INCREF(params);
			if (PyList_SetItem(enclosing_list, 0, params) == -1) {
				Py_DECREF(params);
				Py_DECREF(enclosing_list);
				return NULL;
			}
		}
		else {
			enclosing_list = PyList_New(0);
			if (enclosing_list == NULL) {
				return NULL;
			}
		}
		return enclosing_list;
	}
	else if (multiparam_size == 1) {
		zero_element = PyTuple_GetItem(multiparams, 0);
		if (PyTuple_Check(zero_element) || PyList_Check(zero_element)) {
			zero_element_length = PySequence_Length(zero_element);

			if (zero_element_length != 0) {
				zero_element_item = PySequence_GetItem(zero_element, 0);
				if (zero_element_item == NULL) {
					return NULL;
				}
			}
			else {
				zero_element_item = NULL;
			}

			if (zero_element_length == 0 ||
					(
						PyObject_HasAttrString(zero_element_item, "__iter__") &&
						!PyObject_HasAttrString(zero_element_item, "strip")
					)
				) {
				/*
				 * execute(stmt, [{}, {}, {}, ...])
        		 * execute(stmt, [(), (), (), ...])
				 */
				Py_XDECREF(zero_element_item);
				Py_INCREF(zero_element);
				return zero_element;
			}
			else {
				/*
				 * execute(stmt, ("value", "value"))
				 */
				Py_XDECREF(zero_element_item);
				enclosing_list = PyList_New(1);
				if (enclosing_list == NULL) {
					return NULL;
				}
				Py_INCREF(zero_element);
				if (PyList_SetItem(enclosing_list, 0, zero_element) == -1) {
					Py_DECREF(zero_element);
					Py_DECREF(enclosing_list);
					return NULL;
				}
				return enclosing_list;
			}
		}
		else if (PyObject_HasAttrString(zero_element, "keys")) {
			/*
			 * execute(stmt, {"key":"value"})
			 */
			enclosing_list = PyList_New(1);
			if (enclosing_list ==  NULL) {
				return NULL;
			}
			Py_INCREF(zero_element);
			if (PyList_SetItem(enclosing_list, 0, zero_element) == -1) {
				Py_DECREF(zero_element);
				Py_DECREF(enclosing_list);
				return NULL;
			}
			return enclosing_list;
		} else {
			enclosing_list = PyList_New(1);
			if (enclosing_list ==  NULL) {
				return NULL;
			}
			double_enclosing_list = PyList_New(1);
			if (double_enclosing_list == NULL) {
				Py_DECREF(enclosing_list);
				return NULL;
			}
			Py_INCREF(zero_element);
			if (PyList_SetItem(enclosing_list, 0, zero_element) == -1) {
				Py_DECREF(zero_element);
				Py_DECREF(enclosing_list);
				Py_DECREF(double_enclosing_list);
				return NULL;
			}
			if (PyList_SetItem(double_enclosing_list, 0, enclosing_list) == -1) {
				Py_DECREF(zero_element);
				Py_DECREF(enclosing_list);
				Py_DECREF(double_enclosing_list);
				return NULL;
			}
			return double_enclosing_list;
		}
	}
	else {
		zero_element = PyTuple_GetItem(multiparams, 0);
		if (PyObject_HasAttrString(zero_element, "__iter__") &&
				!PyObject_HasAttrString(zero_element, "strip")
			) {
			Py_INCREF(multiparams);
			return multiparams;
		}
		else {
			enclosing_list = PyList_New(1);
			if (enclosing_list ==  NULL) {
				return NULL;
			}
			Py_INCREF(multiparams);
			if (PyList_SetItem(enclosing_list, 0, multiparams) == -1) {
				Py_DECREF(multiparams);
				Py_DECREF(enclosing_list);
				return NULL;
			}
			return enclosing_list;
		}
	}
}

static PyMethodDef module_methods[] = {
    {"_distill_params", distill_params, METH_VARARGS,
     "Distill an execute() parameter structure."},
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
#endif


#if PY_MAJOR_VERSION >= 3
PyMODINIT_FUNC
PyInit_cutils(void)
#else
PyMODINIT_FUNC
initcutils(void)
#endif
{
    PyObject *m;

#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&module_def);
#else
    m = Py_InitModule3(MODULE_NAME, module_methods, MODULE_DOC);
#endif

#if PY_MAJOR_VERSION >= 3
    if (m == NULL)
        return NULL;
    return m;
#else
    if (m == NULL)
    	return;
#endif
}

