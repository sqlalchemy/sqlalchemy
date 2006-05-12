<%flags>inherit='document_base.myt'</%flags>
<%attr>title='Modules and Classes'</%attr>
<&|doclib.myt:item, name="docstrings", description="Modules and Classes" &>
<%init>
    import cPickle as pickle
    import os
    filename = os.path.join(os.path.dirname(self.file), 'compiled_docstrings.pickle')
    data = pickle.load(file(filename))
</%init>

% for obj in data:
<& pydoc.myt:obj_doc, obj=obj &>
%

</&>