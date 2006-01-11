<%global>
    import re, types
    def format_paragraphs(text):
        return re.sub(r'([\w ])\n([\w ])', r'\1 \2', text or '', re.S)
</%global>

<%method obj_doc>
    <%args>
        obj
        functions = None
        classes = None
    </%args>
    <%init>
        import types
        isclass = isinstance(obj, types.ClassType) or isinstance(obj, types.TypeType)
        name= obj.__name__
        
        if not isclass:
            if hasattr(obj, '__all__'):
                objects = obj.__all__
                sort = True
            else:
                objects = obj.__dict__.keys()
                sort = True
            if functions is None:
                functions = [getattr(obj, x, None) 
                    for x in objects 
                    if getattr(obj,x,None) is not None and 
                        (isinstance(getattr(obj,x), types.FunctionType))
                        and not getattr(obj,x).__name__[0] == '_'
                    ]
                if sort:
                    functions.sort(lambda a, b: cmp(a.__name__, b.__name__))
            if classes is None:
                classes = [getattr(obj, x, None) 
                    for x in objects 
                    if getattr(obj,x,None) is not None and 
                        (isinstance(getattr(obj,x), types.TypeType) 
                        or isinstance(getattr(obj,x), types.ClassType))
                        and not getattr(obj,x).__name__[0] == '_'
                    ]
                if sort:
                    classes.sort(lambda a, b: cmp(a.__name__, b.__name__))
        else:
            if functions is None:
                functions = (
                    [getattr(obj, x).im_func for x in obj.__dict__.keys() if isinstance(getattr(obj,x), types.MethodType) 
                    and 
                    (getattr(obj, x).__name__ == '__init__' or not getattr(obj,x).__name__[0] == '_')
                    ] + 
                    [(x, getattr(obj, x)) for x in obj.__dict__.keys() if isinstance(getattr(obj,x), property) 
                    and 
                    not x[0] == '_'
                    ]
                 )
                functions.sort(lambda a, b: cmp(getattr(a, '__name__', None) or a[0], getattr(b, '__name__', None) or b[0] ))
            if classes is None:
                classes = []
            
        if isclass:
            description = "Class " + name
            if hasattr(obj, '__mro__'):
                description += "(" + obj.__mro__[1].__name__ + ")"
        else:
            description = "Module " + name
    </%init>

<&|doclib.myt:item, name=obj.__name__, description=description &>
<&|formatting.myt:formatplain&><% format_paragraphs(obj.__doc__) %></&>

% if not isclass and len(functions):
<&|doclib.myt:item, name="modfunc", description="Module Functions" &>
<&|formatting.myt:paramtable&>
%   for func in functions:
    <& SELF:function_doc, func=func &>
%
</&>
</&>
% else:
% if len(functions):
<&|formatting.myt:paramtable&>
%   for func in functions:
%   if isinstance(func, types.FunctionType):
    <& SELF:function_doc, func=func &>
%   elif isinstance(func, tuple):
    <& SELF:property_doc, name = func[0], prop=func[1] &>
%
%
</&>
%
%

% if len(classes):
<&|formatting.myt:paramtable&>
%   for class_ in classes:
      <& SELF:obj_doc, obj=class_ &>
%   
</&>
%    
</&>


</%method>

<%method function_doc>
    <%args>func</%args>
    <%init>
        import inspect
        argspec = inspect.getargspec(func)
        argnames = argspec[0]
        varargs = argspec[1]
        varkw = argspec[2]
        defaults = argspec[3] or ()
        argstrings = []
        for i in range(0, len(argnames)):
            if i >= len(argnames) - len(defaults):
                argstrings.append("%s=%s" % (argnames[i], repr(defaults[i - (len(argnames) - len(defaults))])))
            else:
                argstrings.append(argnames[i])
        if varargs is not None:
           argstrings.append("*%s" % varargs)
        if varkw is not None:
           argstrings.append("**%s" % varkw)
    </%init>
    
    <&| formatting.myt:function_doc, name="def " + func.__name__, arglist=argstrings &>
    <&|formatting.myt:formatplain&><% format_paragraphs(func.__doc__) %></&>
    </&>
</%method>


<%method property_doc>
    <%args>
        name
        prop
    </%args>
    <&| formatting.myt:member_doc, name=name + " = property()" &>
    <&|formatting.myt:formatplain&><% format_paragraphs(prop.__doc__) %></&>
    </&>    
</%method>