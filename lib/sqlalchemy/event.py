from sqlalchemy import util

def listen(fn, identifier, target, *args):
    """Listen for events, passing to fn."""
    
    target._dispatch.append(fn, identifier, target, *args)

NO_RESULT = util.symbol('no_result')


class Dispatch(object):
        
    def append(self, identifier, fn, target):
        getattr(self, identifier).append(fn)
    
    def __getattr__(self, key):
        self.__dict__[key] = coll = []
        return coll
    
    def chain(self, identifier, chain_kw, **kw):
        ret = NO_RESULT
        for fn in getattr(self, identifier):
            ret = fn(**kw)
            kw['chain_kw'] = ret
        return ret
            
    def __call__(self, identifier, **kw):
        for fn in getattr(self, identifier):
            fn(**kw)
        
        
class dispatcher(object):
    def __init__(self, dispatch_cls=Dispatch):
        self.dispatch_cls = dispatch_cls
        self._dispatch = dispatch_cls()
        
    def __get__(self, obj, cls):
        if obj is None:
            return self._dispatch
        obj.__dict__['_dispatch'] = disp = self.dispatch_cls()
        for key in self._dispatch.__dict__:
            if key.startswith('on_'):
                disp.__dict__[key] = self._dispatch.__dict__[k].copy()
        return disp
