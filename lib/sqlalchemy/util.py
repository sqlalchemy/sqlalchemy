
__ALL__ = ['OrderedProperties', 'OrderedDict']
import thread

class OrderedProperties(object):

    def __init__(self):
        self.__dict__['_list'] = []
    
    def keys(self):
        return self._list
        
    def __iter__(self):
        return iter([self[x] for x in self._list])
    
    def __setitem__(self, key, object):
        setattr(self, key, object)
        
    def __getitem__(self, key):
        return getattr(self, key)
        
    def __setattr__(self, key, object):
        if not hasattr(self, key):
            self._list.append(key)
    
        self.__dict__[key] = object
    

class OrderedDict(dict):
    """A Dictionary that keeps its own internal ordering"""
    def __init__(self, values = None):
        self.list = []
        if values is not None:
            for val in values:
                self.update(val)

    def keys(self):
        return self.list
    
    def update(self, dict):
        for key in dict.keys():
            self.__setitem__(key, dict[key])
    
    def setdefault(self, key, value):
        if not self.has_key(key):
            self.__setitem__(key, value)
            return value
        else:
            return self.__getitem__(key)
            
    def values(self):
        return map(lambda key: self[key], self.list)
        
    def __iter__(self):
        return iter(self.list)

    def itervalues(self):
        return iter([self[key] for key in self.list])
        
    def iterkeys(self):return self.__iter__()
    
    def iteritems(self):
        return iter([(key, self[key]) for key in self.keys()])
        
    def __setitem__(self, key, object):
        if not self.has_key(key):
            self.list.append(key)

        dict.__setitem__(self, key, object)
        
    def __getitem__(self, key):
        return dict.__getitem__(self, key)


class ThreadLocal(object):
    def __init__(self):
        object.__setattr__(self, 'tdict', {})

    def __getattribute__(self, key):
        try:
            return object.__getattribute__(self, 'tdict')["%d_%s" % (thread.get_ident(), key)]
        except KeyError:
            raise AttributeError(key)
        
    def __setattr__(self, key, value):
        object.__getattribute__(self, 'tdict')["%d_%s" % (thread.get_ident(), key)] = value
        
