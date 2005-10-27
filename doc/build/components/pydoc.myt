<%method obj_doc>
    <%args>
        obj
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
            functions = [getattr(obj, x, None) 
                for x in objects 
                if getattr(obj,x,None) is not None and 
                    (isinstance(getattr(obj,x), types.FunctionType))
                    and not getattr(obj,x).__name__[0] == '_'
                ]
            classes = [getattr(obj, x, None) 
                for x in objects 
                if getattr(obj,x,None) is not None and 
                    (isinstance(getattr(obj,x), types.TypeType) 
                    or isinstance(getattr(obj,x), types.ClassType))
                    and not getattr(obj,x).__name__[0] == '_'
                ]
        else:
            functions = [getattr(obj, x).im_func for x in obj.__dict__.keys() if isinstance(getattr(obj,x), types.MethodType) and not getattr(obj,x).__name__[0] == '_']
            classes = []
            
        functions.sort(lambda a, b: cmp(a.__name__, b.__name__))
        classes.sort(lambda a, b: cmp(a.__name__, b.__name__))
    </%init>

<h2>
% if isclass:
    Class <% name %>
% else:
    Module <% name %>
%
</h2>
<% obj.__doc__ %>
<br/>

<&|formatting.myt:paramtable&>
% if not isclass and len(functions):
    <h3>Module Functions</h3>
%   for func in functions:
hi
    <& SELF:function_doc, func=func &>
%
%

% if len(classes):
    <h3>Classes</h3>
%   for class_ in classes:
      <& SELF:obj_doc, obj=class_ &>
%   
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
        i = 0
        for arg in argnames:
            try:
                default = defaults[i]
                argstrings.append("%s=%s" % (arg, repr(default)))
                i +=1
            except IndexError:
                argstrings.append(arg)
        if varargs is not None:
           argstrings.append("*%s" % varargs)
        if varkw is not None:
           argstrings.append("**%s" % varkw)
    </%init>
    
    huh ? <% repr(func) |h %>
    <&| formatting.myt:function_doc, name="def " + func.__name__, arglist=argstrings &>
    <% func.__doc__ %>
    </&>
</%method>