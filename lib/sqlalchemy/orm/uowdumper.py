# orm/uowdumper.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Dumps out a string representation of a UOWTask structure"""

from sqlalchemy.orm import unitofwork
from sqlalchemy.orm import util as mapperutil
from sqlalchemy import util

class UOWDumper(unitofwork.UOWExecutor):
    def __init__(self, tasks, buf, verbose=False):
        self.verbose = verbose
        self.indent = 0
        self.tasks = tasks
        self.buf = buf
        self.headers = {}
        self.execute(None, tasks)

    def execute(self, trans, tasks, isdelete=None):
        if isdelete is not True:
            for task in tasks:
                self._execute(trans, task, False)
        if isdelete is not False:
            for task in util.reversed(tasks):
                self._execute(trans, task, True)

    def _execute(self, trans, task, isdelete):
        try:
            i = self._indent()
            if i:
                i = i[:-1] + "+-"
            self.buf.write(i + " " + self._repr_task(task))
            self.buf.write(" (" + (isdelete and "delete " or "save/update ") + "phase) \n")
            self.indent += 1
            super(UOWDumper, self).execute(trans, [task], isdelete)
        finally:
            self.indent -= 1


    def save_objects(self, trans, task):
        # sort elements to be inserted by insert order
        def comparator(a, b):
            if a.state is None:
                x = None
            elif not hasattr(a.state, 'insert_order'):
                x = None
            else:
                x = a.state.insert_order
            if b.state is None:
                y = None
            elif not hasattr(b.state, 'insert_order'):
                y = None
            else:
                y = b.state.insert_order
            return cmp(x, y)

        l = list(task.polymorphic_tosave_elements)
        l.sort(comparator)
        for rec in l:
            if rec.listonly:
                continue
            self.header("Save elements"+ self._inheritance_tag(task))
            self.buf.write(self._indent()[:-1] + "+-" + self._repr_task_element(rec)  + "\n")
            self.closeheader()

    def delete_objects(self, trans, task):
        for rec in task.polymorphic_todelete_elements:
            if rec.listonly:
                continue
            self.header("Delete elements"+ self._inheritance_tag(task))
            self.buf.write(self._indent() + "- " + self._repr_task_element(rec)  + "\n")
            self.closeheader()

    def _inheritance_tag(self, task):
        if not self.verbose:
            return ""
        else:
            return (" (inheriting task %s)" % self._repr_task(task))

    def header(self, text):
        """Write a given header just once."""

        if not self.verbose:
            return
        try:
            self.headers[text]
        except KeyError:
            self.buf.write(self._indent() +  "- " + text + "\n")
            self.headers[text] = True

    def closeheader(self):
        if not self.verbose:
            return
        self.buf.write(self._indent() + "- ------\n")

    def execute_dependency(self, transaction, dep, isdelete):
        self._dump_processor(dep, isdelete)

    def execute_save_steps(self, trans, task):
        super(UOWDumper, self).execute_save_steps(trans, task)

    def execute_delete_steps(self, trans, task):
        super(UOWDumper, self).execute_delete_steps(trans, task)

    def execute_dependencies(self, trans, task, isdelete=None):
        super(UOWDumper, self).execute_dependencies(trans, task, isdelete)

    def execute_cyclical_dependencies(self, trans, task, isdelete):
        self.header("Cyclical %s dependencies" % (isdelete and "delete" or "save"))
        super(UOWDumper, self).execute_cyclical_dependencies(trans, task, isdelete)
        self.closeheader()

    def execute_per_element_childtasks(self, trans, task, isdelete):
        super(UOWDumper, self).execute_per_element_childtasks(trans, task, isdelete)

    def execute_element_childtasks(self, trans, element, isdelete):
        self.header("%s subelements of UOWTaskElement(%s)" % ((isdelete and "Delete" or "Save"), hex(id(element))))
        super(UOWDumper, self).execute_element_childtasks(trans, element, isdelete)
        self.closeheader()

    def _dump_processor(self, proc, deletes):
        if deletes:
            val = proc.targettask.polymorphic_todelete_elements
        else:
            val = proc.targettask.polymorphic_tosave_elements

        if self.verbose:
            self.buf.write(self._indent() + "   +- %s attribute on %s (UOWDependencyProcessor(%d) processing %s)\n" % (
                repr(proc.processor.key),
                    ("%s's to be %s" % (self._repr_task_class(proc.targettask), deletes and "deleted" or "saved")),
                hex(id(proc)),
                self._repr_task(proc.targettask))
            )
        elif False:
            self.buf.write(self._indent() + "   +- %s attribute on %s\n" % (
                repr(proc.processor.key),
                    ("%s's to be %s" % (self._repr_task_class(proc.targettask), deletes and "deleted" or "saved")),
                )
            )

        if len(val) == 0:
            if self.verbose:
                self.buf.write(self._indent() + "   +- " + "(no objects)\n")
        for v in val:
            self.buf.write(self._indent() + "   +- " + self._repr_task_element(v, proc.processor.key, process=True) + "\n")

    def _repr_task_element(self, te, attribute=None, process=False):
        if getattr(te, 'state', None) is None:
            objid = "(placeholder)"
        else:
            if attribute is not None:
                objid = "%s.%s" % (mapperutil.state_str(te.state), attribute)
            else:
                objid = mapperutil.state_str(te.state)
        if self.verbose:
            return "%s (UOWTaskElement(%s, %s))" % (objid, hex(id(te)), (te.listonly and 'listonly' or (te.isdelete and 'delete' or 'save')))
        elif process:
            return "Process %s" % (objid)
        else:
            return "%s %s" % ((te.isdelete and "Delete" or "Save"), objid)

    def _repr_task(self, task):
        if task.mapper is not None:
            if task.mapper.__class__.__name__ == 'Mapper':
                name = task.mapper.class_.__name__ + "/" + task.mapper.local_table.description + "/" + str(task.mapper.entity_name)
            else:
                name = repr(task.mapper)
        else:
            name = '(none)'
        sd = getattr(task, '_superduper', False)
        if sd:
            return ("SD UOWTask(%s, %s)" % (hex(id(task)), name))
        else:
            return ("UOWTask(%s, %s)" % (hex(id(task)), name))

    def _repr_task_class(self, task):
        if task.mapper is not None and task.mapper.__class__.__name__ == 'Mapper':
            return task.mapper.class_.__name__
        else:
            return '(none)'

    def _indent(self):
        return "   |" * self.indent
