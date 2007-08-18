"""define opeators used in SQL expressions"""

from operator import and_, or_, inv, add, mul, sub, div, mod, truediv, lt, le, ne, gt, ge, eq

def from_():
    raise NotImplementedError()

def as_():
    raise NotImplementedError()

def exists():
    raise NotImplementedError()

def is_():
    raise NotImplementedError()

def isnot():
    raise NotImplementedError()

def like_op(a, b):
    return a.like(b)

def notlike_op(a, b):
    raise NotImplementedError()

def ilike_op(a, b):
    return a.ilike(b)

def notilike_op(a, b):
    raise NotImplementedError()

def between_op(a, b):
    return a.between(b)

def in_op(a, b):
    return a.in_(*b)

def notin_op(a, b):
    raise NotImplementedError()

def distinct_op(a):
    return a.distinct()

def startswith_op(a, b):
    return a.startswith(b)

def endswith_op(a, b):
    return a.endswith(b)

def comma_op(a, b):
    raise NotImplementedError()

def concat_op(a, b):
    return a.concat(b)

def desc_op(a):
    return a.desc()

def asc_op(a):
    return a.asc()

