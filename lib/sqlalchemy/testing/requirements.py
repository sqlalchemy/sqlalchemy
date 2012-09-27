"""Global database feature support policy.

Provides decorators to mark tests requiring specific feature support from the
target database.

"""

from .exclusions import \
     skip, \
     skip_if,\
     only_if,\
     only_on,\
     fails_on,\
     fails_on_everything_except,\
     fails_if,\
     SpecPredicate,\
     against

def no_support(db, reason):
    return SpecPredicate(db, description=reason)

def exclude(db, op, spec, description=None):
    return SpecPredicate(db, op, spec, description=description)


def _chain_decorators_on(*decorators):
    def decorate(fn):
        for decorator in reversed(decorators):
            fn = decorator(fn)
        return fn
    return decorate

class Requirements(object):
    def __init__(self, db, config):
        self.db = db
        self.config = config


