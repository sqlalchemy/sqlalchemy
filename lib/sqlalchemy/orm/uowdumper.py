
"""dumps out a string representation of a UOWTask structure"""

class UOWDumper(object):
    def __init__(self, task, buf, verbose=False):
        self.verbose = verbose
        self.indent = 0
        self.task = task
        self.buf = buf
        self._dump(task)
        
    def _dump_processor(self, proc, deletes):
        if deletes:
            val = proc.targettask.polymorphic_todelete_elements
        else:
            val = proc.targettask.polymorphic_tosave_elements

        if self.verbose:
            self.buf.write(self._indent() + "  |- %s attribute on %s (UOWDependencyProcessor(%d) processing %s)\n" % (
                repr(proc.processor.key), 
                    ("%s's to be %s" % (self._repr_task_class(proc.targettask), deletes and "deleted" or "saved")),
                id(proc), 
                self._repr_task(proc.targettask))
            )
        elif False:
            self.buf.write(self._indent() + "  |- %s attribute on %s\n" % (
                repr(proc.processor.key), 
                    ("%s's to be %s" % (self._repr_task_class(proc.targettask), deletes and "deleted" or "saved")),
                )
            )
            
        if len(val) == 0:
            if self.verbose:
                self.buf.write(self._indent() + "  |       |-" + "(no objects)\n")
        for v in val:
            self.buf.write(self._indent() + "  |       |-" + self._repr_task_element(v, proc.processor.key, process=True) + "\n")

    def _repr_task_element(self, te, attribute=None, process=False):
        if te.obj is None:
            objid = "(placeholder)"
        else:
            if attribute is not None:
                objid = "%s(%d).%s" % (te.obj.__class__.__name__, id(te.obj), attribute)
            else:
                objid = "%s(%d)" % (te.obj.__class__.__name__, id(te.obj))
        if self.verbose:
            return "%s (UOWTaskElement(%d, %s))" % (objid, id(te), (te.listonly and 'listonly' or (te.isdelete and 'delete' or 'save')))
        elif process:
            return "Process %s" % (objid)
        else:
            return "%s %s" % ((te.isdelete and "Delete" or "Save"), objid)

    def _repr_task(self, task):
        if task.mapper is not None:
            if task.mapper.__class__.__name__ == 'Mapper':
                name = task.mapper.class_.__name__ + "/" + task.mapper.local_table.name + "/" + str(task.mapper.entity_name)
            else:
                name = repr(task.mapper)
        else:
            name = '(none)'
        return ("UOWTask(%d, %s)" % (id(task), name))
        
    def _repr_task_class(self, task):
        if task.mapper is not None and task.mapper.__class__.__name__ == 'Mapper':
            return task.mapper.class_.__name__
        else:
            return '(none)'

    def _repr(self, obj):
        return "%s(%d)" % (obj.__class__.__name__, id(obj))

    def _indent(self):
        return "  | " * self.indent

    def _dump(self, starttask, indent=None, circularparent=None):
        try:
            oldindent = self.indent
            if indent is not None:
                self.indent = indent
            self._dump_impl(starttask, circularparent=circularparent)
        finally:
            self.indent = oldindent
            
    def _dump_impl(self, starttask, circularparent=None):

        headers = {}
        def header(buf, text):
            """writes a given header just once"""
            try:
                headers[text]
            except KeyError:
                self.buf.write(self._indent() + "  |\n")
                self.buf.write(text)
                headers[text] = True

        def _inheritance_tag(task):
            if not self.verbose:
                return ""
            elif task is not starttask:
                return (" (inheriting task %s)" % self._repr_task(task))
            else:
                return ""

        def _dump_saveelements(task):
            for ptask in task.polymorphic_tasks():
                for rec in ptask.tosave_elements:
                    if rec.listonly:
                        continue
                    if self.verbose:
                        header(self.buf, self._indent() + "  |- Save elements"+ _inheritance_tag(task) + "\n")
                    self.buf.write(self._indent() + "  |- " + self._repr_task_element(rec)  + "\n")

        def _dump_deleteelements(task):
            for ptask in task.polymorphic_tasks():
                for rec in ptask.todelete_elements:
                    if rec.listonly:
                        continue
                    if self.verbose:
                        header(self.buf, self._indent() + "  |- Delete elements"+ _inheritance_tag(ptask) + "\n")
                    self.buf.write(self._indent() + "  |- " + self._repr_task_element(rec) + "\n")

        def _dump_dependencies(task):
            alltasks = list(task.polymorphic_tasks())
            for task in alltasks:
                for dep in task.dependencies:
                    if self.verbose:
                        header(self.buf, self._indent() + "  |- Save dependencies" + _inheritance_tag(task) + "\n")
                    self._dump_processor(dep, False)
            alltasks.reverse()
            for task in alltasks:
                for dep in task.dependencies:
                    if self.verbose:
                        header(self.buf, self._indent() + "  |- Delete dependencies" + _inheritance_tag(task) + "\n")
                    self._dump_processor(dep, True)
    
        def _dump_childtasks(task):
            for ptask in task.polymorphic_tasks():
                for child in ptask.childtasks:
                    if self.verbose:
                        header(self.buf, self._indent() + "  |- Child tasks" + _inheritance_tag(task) + "\n")
                    self._dump(child, indent = self.indent + 1)
        
        if starttask.circular is not None:
            self._dump(starttask.circular, indent=self.indent, circularparent=starttask)
            return

        i = self._indent()
        if len(i):
            i = i[0:-1] + "-"
        if circularparent is not None:
            self.buf.write(self._indent() + "\n")
            self.buf.write(i + " " + self._repr_task(circularparent))
            self.buf.write("->circular->" + self._repr_task(starttask))
        else:
            self.buf.write(self._indent() + "\n")
            self.buf.write(i + " " + self._repr_task(starttask))
        
        self.buf.write("\n")
        _dump_saveelements(starttask)
        for dep in starttask.cyclical_dependencies:
            if self.verbose:
                header(self.buf, self._indent() + "  |- Cyclical Save dependencies\n")
            self._dump_processor(dep, False)
        for element in starttask.tosave_elements:
            for task in element.childtasks:
                if self.verbose:
                    header(self.buf, self._indent() + "  |- Save subelements of UOWTaskElement(%s)\n" % id(element))
                self._dump(task, indent = self.indent + 1)
        _dump_dependencies(starttask)
        for dep in starttask.cyclical_dependencies:
            if self.verbose:
                header(self.buf, self._indent() + "  |- Cyclical Delete dependencies\n")
            self._dump_processor(dep, True)
        _dump_childtasks(starttask)
        for element in starttask.todelete_elements:
            for task in element.childtasks:
                if self.verbose:
                    header(self.buf, self._indent() + "  |- Delete subelements of UOWTaskElement(%s)\n" % id(element))
                self._dump(task, indent = self.indent + 1)
        _dump_deleteelements(starttask)

        if starttask.is_empty():   
            self.buf.write(self._indent() + "  |- (empty task)\n")
        else:
            self.buf.write(self._indent() + "  |----\n")
        
        self.buf.write(self._indent() + "\n")           
