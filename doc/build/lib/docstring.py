import re, types, string, inspect

"""sucks a module and its contents into a simple documentation object, suitable for pickling"""

class ObjectDoc(object):
    def __init__(self, obj, functions=None, classes=None):
        self.isclass = isinstance(obj, types.ClassType) or isinstance(obj, types.TypeType)
        self.name= obj.__name__
        functions = functions
        classes= classes
        
        if not self.isclass:
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
                classes = [getattr(obj, x, None) for x in objects 
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
        
        if self.isclass:
            self.description = "Class " + self.name
            if hasattr(obj, '__mro__'):
                l = []
                mro = list(obj.__mro__[1:])
                mro.reverse()
                for x in mro:
                    for y in x.__mro__[1:]:
                        if y in l:
                            del l[l.index(y)]
                    l.insert(0, x)
                self.description += "(" + string.join([x.__name__ for x in l], ',') + ")"
        else:
            self.description = "Module " + self.name

        self.doc = obj.__doc__

        self.functions = []
        if not self.isclass and len(functions):
            for func in functions:
                self.functions.append(FunctionDoc(func))
        else:
            if len(functions):
                for func in functions:
                    if isinstance(func, types.FunctionType):
                        self.functions.append(FunctionDoc(func))
                    elif isinstance(func, tuple):
                        self.functions.append(PropertyDoc(func[0], func[1]))
                        
        self.classes = []
        for class_ in classes:
            self.classes.append(ObjectDoc(class_))

    def accept_visitor(self, visitor):
        visitor.visit_object(self)

class FunctionDoc(object):
    def __init__(self, func):
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
        self.argstrings = self.arglist = argstrings
        self.name = "def " + func.__name__
        self.link = func.__name__
        self.doc = func.__doc__
    def accept_visitor(self, visitor):
        visitor.visit_function(self)

class PropertyDoc(object):
    def __init__(self, name, prop):
        self.doc = prop.__doc__
        self.name = name + " = property()"
        self.link = name
    def accept_visitor(self, visitor):
        visitor.visit_property(self)
