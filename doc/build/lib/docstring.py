"""
defines a pickleable, recursive "generated python documentation" datastructure.
"""

import operator, re, types, string, inspect

allobjects = {}

class AbstractDoc(object):
    def __init__(self, obj):
        allobjects[id(obj)] = self
        self.id = id(obj)
        self.allobjects = allobjects
        self.toc_path = None

class ObjectDoc(AbstractDoc):
    def __init__(self, obj, functions=None, classes=None, include_all_classes=False):
        super(ObjectDoc, self).__init__(obj)
        self.isclass = isinstance(obj, types.ClassType) or isinstance(obj, types.TypeType)
        self.name= obj.__name__
        self.include_all_classes = include_all_classes
        functions = functions
        classes= classes

        if not self.isclass:
            if not include_all_classes and hasattr(obj, '__all__'):
                objects = obj.__all__
                sort = True
            else:
                objects = obj.__dict__.keys()
                sort = True
            if functions is None:
                functions = [
                    (x, getattr(obj, x, None))
                    for x in objects
                    if getattr(obj,x,None) is not None and
                        (isinstance(getattr(obj,x), types.FunctionType))
                        and not self._is_private_name(getattr(obj,x).__name__)]
                if sort:
                    functions.sort(key=operator.itemgetter(0))
            if classes is None:
                classes = [getattr(obj, x, None) for x in objects
                    if getattr(obj,x,None) is not None and
                        (isinstance(getattr(obj,x), types.TypeType)
                        or isinstance(getattr(obj,x), types.ClassType))
                        and (self.include_all_classes or not self._is_private_name(getattr(obj,x).__name__))
                    ]
                classes = list(set(classes))
                if sort:
                    classes.sort(lambda a, b: cmp(a.__name__.replace('_', ''), b.__name__.replace('_', '')))
        else:
            if functions is None:
                methods = [
                    (x, getattr(obj, x).im_func)
                    for x in obj.__dict__.keys()
                    if (isinstance(getattr(obj,x), types.MethodType) and
                        (getattr(obj, x).__name__ == '__init__' or
                         not self._is_private_name(x)))]
                props = [
                    (x, getattr(obj, x))
                    for x in obj.__dict__.keys()
                    if (_is_property(getattr(obj,x)) and
                        not self._is_private_name(x))]

                functions = methods + props
                functions.sort(_method_sort)
            if classes is None:
                classes = []

        if self.isclass:
            self.description = "class " + self.name
            self.classname = self.name
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
                self._inherits = [(id(x), x.__name__) for x in l]
            else:
                self._inherits = []
        else:
            self.description = "module " + self.name

        self.doc = obj.__doc__

        self.functions = []

        for name, func in functions:
            if isinstance(func, types.FunctionType):
                if self.isclass:
                    self.functions.append(MethodDoc(name, func, self))
                else:
                    self.functions.append(FunctionDoc(name, func))
            else:
                self.functions.append(PropertyDoc(name, func))

        self.classes = []
        for class_ in classes:
            self.classes.append(ObjectDoc(class_))

    def _is_private_name(self, name):
        if name in ('__weakref__', '__repr__','__str__', '__unicode__',
                    '__getstate__', '__setstate__', '__reduce__',
                    '__reduce_ex__', '__hash__'):
            return True
        elif re.match(r'^__.*__$', name):
            return False
        elif name.startswith('_'):
            return True
        else:
            return False

    def _get_inherits(self):
        for item in self._inherits:
            if item[0] in self.allobjects:
                yield self.allobjects[item[0]]
            else:
                yield item[1]
    inherits = property(_get_inherits)
    def accept_visitor(self, visitor):
        visitor.visit_object(self)

def _is_property(elem):
    return isinstance(elem, property) or (hasattr(elem, '__get__') and hasattr(elem, '__set__'))

class FunctionDoc(AbstractDoc):
    def __init__(self, name, func):
        super(FunctionDoc, self).__init__(func)
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
        self.name = name
        self.link = func.__name__
        self.doc = func.__doc__
    def accept_visitor(self, visitor):
        visitor.visit_function(self)

class MethodDoc(FunctionDoc):
    def __init__(self, name, func, owner):
        super(MethodDoc, self).__init__(name, func)
        if name == '__init__' and not self.doc:
            self.doc = "Construct a new ``%s``." % owner.name

class PropertyDoc(AbstractDoc):
    def __init__(self, name, prop):
        super(PropertyDoc, self).__init__(prop)
        self.doc = prop.__doc__
        self.name = name
        self.link = name
    def accept_visitor(self, visitor):
        visitor.visit_property(self)

def _method_sort(speca, specb):
    a = getattr(speca[1], '__name__', speca[0])
    b = getattr(specb[1], '__name__', speca[0])

    if a == '__init__': return -1
    if b == '__init__': return 1

    a_u = a.startswith('__') and a.endswith('__')
    b_u = b.startswith('__') and b.endswith('__')

    if a_u and not b_u: return 1
    if b_u and not a_u: return -1

    return cmp(a, b)
