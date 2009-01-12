# orm/uowdumper.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Dumps out a string representation of a UOWTask structure"""

from sqlalchemy.orm import unitofwork
from sqlalchemy.orm import util as mapperutil
import StringIO

class UOWDumper(unitofwork.UOWExecutor):
    def __init__(self, tasks, buf):
        self.indent = 0
        self.tasks = tasks
        self.buf = buf
        self.execute(None, tasks)

    @classmethod
    def dump(cls, tasks):
        buf = StringIO.StringIO()
        UOWDumper(tasks, buf)
        return buf.getvalue()

    def execute(self, trans, tasks, isdelete=None):
        if isdelete is not True:
            for task in tasks:
                self._execute(trans, task, False)
        if isdelete is not False:
            for task in reversed(tasks):
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
        for rec in sorted(task.polymorphic_tosave_elements, key=lambda a: a.state.sort_key):
            if rec.listonly:
                continue
            self.buf.write(self._indent()[:-1] + "+-" + self._repr_task_element(rec)  + "\n")

    def delete_objects(self, trans, task):
        for rec in task.polymorphic_todelete_elements:
            if rec.listonly:
                continue
            self.buf.write(self._indent() + "- " + self._repr_task_element(rec)  + "\n")

    def execute_dependency(self, transaction, dep, isdelete):
        self._dump_processor(dep, isdelete)

    def _dump_processor(self, proc, deletes):
        if deletes:
            val = proc.targettask.polymorphic_todelete_elements
        else:
            val = proc.targettask.polymorphic_tosave_elements

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
        if process:
            return "Process %s" % (objid)
        else:
            return "%s %s" % ((te.isdelete and "Delete" or "Save"), objid)

    def _repr_task(self, task):
        if task.mapper is not None:
            if task.mapper.__class__.__name__ == 'Mapper':
                name = task.mapper.class_.__name__ + "/" + task.mapper.local_table.description
            else:
                name = repr(task.mapper)
        else:
            name = '(none)'
        return ("UOWTask(%s, %s)" % (hex(id(task)), name))

    def _repr_task_class(self, task):
        if task.mapper is not None and task.mapper.__class__.__name__ == 'Mapper':
            return task.mapper.class_.__name__
        else:
            return '(none)'

    def _indent(self):
        return "   |" * self.indent
