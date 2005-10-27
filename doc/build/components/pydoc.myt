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
            if hasattr(obj, '__ALL__'):
                objects = obj.__ALL__
            else:
                objects = obj.__dict__.keys()
            if functions is None:
                functions = [getattr(obj, x, None) 
                    for x in objects 
                    if getattr(obj,x,None) is not None and 
                        (isinstance(getattr(obj,x), types.FunctionType))
                        and not getattr(obj,x).__name__[0] == '_'
                    ]
                functions.sort(lambda a, b: cmp(a.__name__, b.__name__))
            if classes is None:
                classes = [getattr(obj, x, None) 
                    for x in objects 
                    if getattr(obj,x,None) is not None and 
                        (isinstance(getattr(obj,x), types.TypeType) 
                        or isinstance(getattr(obj,x), types.ClassType))
                        and not getattr(obj,x).__name__[0] == '_'
                    ]
                classes.sort(lambda a, b: cmp(a.__name__, b.__name__))
        else:
            if functions is None:
                functions = [getattr(obj, x).im_func for x in obj.__dict__.keys() if isinstance(getattr(obj,x), types.MethodType) and not getattr(obj,x).__name__[0] == '_']
                functions.sort(lambda a, b: cmp(a.__name__, b.__name__))
            if classes is None:
                classes = []
            
        if isclass:
            description = "Class " + name
        else:
            description = "Module " + name
    </%init>

<&|doclib.myt:item, name=obj.__name__, description=description &>

<% obj.__doc__ %>
<% (obj.__doc__ and "<br/><br/>" or '') %>

% if len(functions):
<&|formatting.myt:paramtable&>
%   for func in functions:
    <& SELF:function_doc, func=func &>
%
</&>
%

% if len(classes):
    <h3>Classes</h3>
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
    <% func.__doc__ %>
    </&>
</%method>