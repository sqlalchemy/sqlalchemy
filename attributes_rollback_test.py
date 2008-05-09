from sqlalchemy.orm import attributes
class Foo(object):pass
attributes.register_class(Foo)
attributes.register_attribute(Foo, 'x', uselist=False, useobject=False, mutable_scalars=True, copy_function=lambda x:x.copy())

f = Foo()
f._foostate.set_savepoint()
print f._foostate.get_history('x')

f.x = {'1':15}


print f._foostate.get_history('x')
f._foostate.commit_all()

print f._foostate.get_history('x')

f.x['2'] = 40
print f._foostate.get_history('x')

f._foostate.rollback()

print f._foostate.get_history('x')

#import pdb
#pdb.Pdb().break_here()

print f.x
f.x['2'] = 40
print f._foostate.get_history('x')

